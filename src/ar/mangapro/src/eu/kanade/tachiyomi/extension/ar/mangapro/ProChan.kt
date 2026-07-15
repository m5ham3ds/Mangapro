package eu.kanade.tachiyomi.extension.ar.mangapro

import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.Canvas
import android.graphics.Rect
import android.util.Base64
import android.util.Log
import eu.kanade.tachiyomi.network.GET
import eu.kanade.tachiyomi.network.POST
import eu.kanade.tachiyomi.source.model.Filter
import eu.kanade.tachiyomi.source.model.FilterList
import eu.kanade.tachiyomi.source.model.MangasPage
import eu.kanade.tachiyomi.source.model.Page
import eu.kanade.tachiyomi.source.model.SChapter
import eu.kanade.tachiyomi.source.model.SManga
import eu.kanade.tachiyomi.source.online.HttpSource
import keiyoushi.lib.cookieinterceptor.CookieInterceptor
import keiyoushi.utils.extractNextJs
import keiyoushi.utils.firstInstance
import keiyoushi.utils.parseAs
import keiyoushi.utils.toJsonString
import keiyoushi.utils.tryParse
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Headers
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.Interceptor
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import okhttp3.internal.closeQuietly
import okio.IOException
import rx.Observable
import tachiyomi.decoder.ImageDecoder
import java.io.ByteArrayOutputStream
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.TimeZone
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

class ProChan : HttpSource() {
    override val name = "ProChan"
    override val lang = "ar"
    // ملاحظة مهمة حول النطاقات (مؤكدة الآن من فحص مصدر صفحة حقيقية فعلياً، لا افتراضاً):
    // - procomic.pro هو النطاق الذي يحتوي فعلياً على بيانات JSON الكاملة المُضمَّنة داخل
    //   الصفحة (الفصول initialChapters وتفاصيل السلسلة series) — وهذه البيانات هي ما
    //   تعتمد عليه طريقة عملنا بالكامل (استخراج JSON من الـ HTML مباشرة، دون استدعاء API
    //   منفصل صراحة).
    // - procomic.net يُقدّم لنفس مسار السلسلة نسخة "/info" (صفحة معلومات/تقييمات فقط)
    //   لا تحتوي على initialChapters ولا series إطلاقاً — وقد تأكد هذا فعلياً بفحص مصدر
    //   صفحة حقيقية لم تحتوِ ولا مرة واحدة على "initialChapters". والأهم: تلك الصفحة نفسها
    //   تتضمن رابطاً داخلياً صريحاً يوجّه إلى procomic.pro لعرض السلسلة الكاملة — أي أن
    //   الموقع نفسه يُعامل procomic.net كمرآة عرض/فهرسة خفيفة فقط، لا كنطاق وظيفي كامل.
    // - لذلك procomic.pro هو الأساس الصحيح دائماً لطلبات تفاصيل السلسلة وقوائم الفصول،
    //   وprocomic.net نطاق احتياطي فقط (قد يفيد لطلبات أخرى كالبحث، لكن لا يُعتمد عليه
    //   لصفحات الفصول تحديداً).
    // - هذا الموقع أيضاً يُغيّر نطاقاته بشكل متكرر جداً تاريخياً (موثّق عبر تقارير مستخدمين
    //   متعددة: promanga.pro → prochan.net → promanga.net → procomic.pro/net)، لذلك آلية
    //   التبديل التلقائي عند 404/410 تبقى ضرورية بغض النظر عن أي شيء آخر.
    private val domain = "procomic.pro"
    private val altDomain = "procomic.net"

    // نطاق صور الأغلفة (CDN) بصيغة "cdn3.procomic.net" ونحوها — نفس نص altDomain، لكن
    // يُستخدم هنا كلاحقة اسم مضيف فرعي لصور الـ CDN تحديداً، لا كنطاق الموقع نفسه.
    private val cdnDomain = "procomic.net"

    override val baseUrl = "https://$domain"
    override val supportsLatest = true
    override val versionId = 15

    private val json = Json { ignoreUnknownKeys = true }

    companion object {
        private const val SCRAMBLED_SCHEME = "https://procomic.pro/__scrambled__/?map="
        private const val TILED_SCHEME = "https://procomic.pro/__tiled__/?map="
        // نفس فكرة TILED_SCHEME لكن للصفحات "العادية" (غير المقسّمة بصرياً): روابطها
        // أيضاً تحمل expires/token قصيري الأجل (تأكدنا من ذلك بفحص عيّنة حقيقية)، وكانت
        // هذه هي بالضبط الصفحات التي تظهر بصورة بديلة/مكررة عشوائياً عند انتهاء توكنها،
        // لأنها لم تكن تمرّ بأي آلية تحديث توكن على عكس الصفحات المقسّمة.
        private const val SIMPLE_SCHEME = "https://procomic.pro/__simple__/?map="
    }

    override val client: OkHttpClient = network.cloudflareClient.newBuilder()
        .addInterceptor(::domainFallbackInterceptor)
        .addInterceptor(::scrambledImageInterceptor)
        .addInterceptor(::tiledImageInterceptor)
        .addInterceptor(::simpleImageInterceptor)
        .addNetworkInterceptor(
            CookieInterceptor(
                domain,
                listOf("safe_browsing" to "off", "language" to "ar")
            )
        )
        .build()

    /**
     * يحاول تلقائياً النطاق البديل [altDomain] كلما أرجع النطاق الأساسي [domain] رمز
     * 404 (غير موجود) أو 410 (اختفى نهائياً) — يحدث هذا أحياناً حين تكون سلسلة معيّنة
     * غير متزامنة بعد بين procomic.pro (الأصلي) وprocomic.net (المرآة)، أو حين يبدّل
     * الموقع نطاقه بالكامل مجدداً كما فعل عدة مرات سابقاً في تاريخه. بهذا لا تنهار
     * الإضافة بالكامل في كل مرة يحدث فيها ذلك.
     */
    private fun domainFallbackInterceptor(chain: Interceptor.Chain): Response {
        val request = chain.request()

        // نستثني روابطنا الداخلية الوهمية (التي تمثّل صوراً مركّبة محلياً ولا تُطلب من
        // الشبكة فعلياً) من أي محاولة تبديل نطاق، فهي ليست طلبات حقيقية للموقع.
        val path = request.url.encodedPath
        if (path.startsWith("/__tiled__") || path.startsWith("/__scrambled__") || path.startsWith("/__simple__")) {
            return chain.proceed(request)
        }

        if (request.url.host != domain) {
            return chain.proceed(request)
        }

        val response = chain.proceed(request)
        if (response.code != 404 && response.code != 410) {
            return response
        }

        // نحتفظ برمز ورسالة الفشل الأصليين قبل إغلاق الاستجابة، لنستخدمهما لاحقاً إن
        // فشل النطاق البديل أيضاً — بدل إعادة إرسال الطلب الأصلي مرة ثالثة بلا فائدة
        // (نتيجته معروفة مسبقاً).
        val originalCode = response.code
        val originalMessage = response.message
        response.close()

        val fallbackUrl = request.url.newBuilder().host(altDomain).build()
        val fallbackRequest = request.newBuilder().url(fallbackUrl).build()

        fun originalFailureResponse() = Response.Builder()
            .request(request)
            .protocol(Protocol.HTTP_1_1)
            .code(originalCode)
            .message(originalMessage)
            .body("".toResponseBody(null))
            .build()

        return try {
            val fallbackResponse = chain.proceed(fallbackRequest)
            if (fallbackResponse.isSuccessful) {
                fallbackResponse
            } else {
                fallbackResponse.close()
                originalFailureResponse()
            }
        } catch (e: Exception) {
            Log.e("ProChan-Debug", "فشلت محاولة النطاق البديل ($altDomain) أيضاً: ${e.message}")
            originalFailureResponse()
        }
    }

    // ======================== ترويسات محسّنة لمحاكاة متصفح هاتف أندرويد ========================
    override fun headersBuilder(): Headers.Builder {
        // ملاحظة: تم حذف ترويسة Origin من هنا لأن المتصفح الحقيقي لا يرسلها في طلبات
        // التصفح العادية (GET Navigation)، إرسالها في كل طلب قد تكون أحد أسباب كشف
        // الإضافة كبوت من طرف نظام الحماية. سنضيفها فقط عند الحاجة (طلب deferred-media).
        // كما تم تعمّد عدم إضافة "Accept-Encoding" يدوياً: OkHttp يتعامل معه تلقائياً،
        // وأي تعيين يدوي له يعطّل فك الضغط (gzip) التلقائي فينتج عنه جسم استجابة تالف.
        return Headers.Builder()
            .set("User-Agent", "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36")
            .set("Referer", "$baseUrl/")
            .set("Accept-Language", "ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7")
            .set("Sec-Ch-Ua", "\"Chromium\";v=\"124\", \"Google Chrome\";v=\"124\", \"Not-A.Brand\";v=\"99\"")
            .set("Sec-Ch-Ua-Mobile", "?1")
            .set("Sec-Ch-Ua-Platform", "\"Android\"")
    }

    private val rscHeaders = headersBuilder()
        .set("rsc", "1")
        .build()

    // بقية الكود كما هو، مع التأكد من استخدام headersBuilder() بشكل صحيح
    // ... (جميع الدوال الأخرى تبقى كما هي، مع تعديل بسيط في imageRequest للتأكد من استخدام build())
    // سأقوم بإعادة كتابة الدوال التي استخدمت headersBuilder() و rscHeaders للتأكد من أنها صحيحة

    // =================================================================
    // POPULAR / LATEST / SEARCH
    // =================================================================
    override fun fetchPopularManga(page: Int): Observable<MangasPage> {
        val filters = getFilterList().apply {
            firstInstance<SortFilter>().state = 2
        }
        return fetchSearchManga(page, "", filters)
    }

    override fun fetchLatestUpdates(page: Int): Observable<MangasPage> {
        val filters = getFilterList().apply {
            firstInstance<SortFilter>().state = 1
        }
        return fetchSearchManga(page, "", filters)
    }

    override fun fetchSearchManga(page: Int, query: String, filters: FilterList): Observable<MangasPage> {
        if (query.startsWith("https://")) {
            val url = query.toHttpUrl()
            // الموقع أصبح يستخدم بادئة لغة اختيارية في الروابط (/ar/series/... أو
            // /en/series/...)، فنتجاوزها هنا إن وُجدت قبل التحقق من بنية الرابط.
            val path = url.pathSegments.let { segments ->
                if (segments.firstOrNull() in setOf("ar", "en")) segments.drop(1) else segments
            }
            if (url.host in setOf(domain, altDomain) && path.size >= 4 && path[0] == "series") {
                val type = path[1]
                if (type !in SUPPORTED_TYPES) throw Exception("نوع غير مدعوم: $type")
                val mangaId = path[2]
                val slug = path[3]
                val manga = SManga.create().apply { this.url = "/series/$type/$mangaId/$slug" }
                return fetchMangaDetails(manga).map { MangasPage(listOf(it), false) }
            } else {
                throw Exception("رابط غير مدعوم")
            }
        }

        return Observable.fromCallable {
            val request = searchMangaRequest(page, query, filters)
            val response = client.newCall(request).execute()
            response.use {
                if (!response.isSuccessful) {
                    if (response.code == 403) {
                        throw Exception("⚠️ تم حظر الوصول (HTTP 403)\n\n🔧 الحل:\n1. اذهب إلى الإعدادات ← الامتدادات ← ProChan ← افتح WebView\n2. تصفح الموقع وافتح أي فصل حتى تظهر الصور (لتجاوز Cloudflare)\n3. ارجع إلى هذه القائمة واسحب لأسفل للتحديث\n4. ثم حاول مرة أخرى.")
                    }
                    throw Exception("HTTP ${response.code}")
                }

                val statusFilter = filters.firstInstance<StatusFilter>().selected
                val genreFilter = filters.firstInstance<GenreFilter>()
                val tagFilter = filters.firstInstance<TagFilter>()

                val data = response.parseAs<MetaData<BrowseManga>>()
                val mangas = data.data.asSequence()
                    .filter { manga -> statusFilter == null || manga.progress == statusFilter }
                    .filter { manga -> genreFilter.included.isEmpty() || manga.metadata.genres.containsAll(genreFilter.included) }
                    .filter { manga -> genreFilter.excluded.none { it in manga.metadata.genres } }
                    .filter { manga -> tagFilter.included.isEmpty() || manga.metadata.tags.containsAll(tagFilter.included) }
                    .filter { manga -> tagFilter.excluded.none { it in manga.metadata.tags } }
                    .map { manga ->
                        SManga.create().apply {
                            url = "/series/${manga.type}/${manga.id}/${manga.slug}"
                            title = manga.title
                            thumbnail_url = (manga.coverImageApp?.desktop ?: manga.coverImage)?.let {
                                if (it.startsWith("/")) manga.cdn?.let { cdn -> "https://$cdn.$cdnDomain$it" } else it
                            }
                        }
                    }.toList()

                MangasPage(mangas, data.meta.hasNextPage())
            }
        }
    }

    override fun searchMangaRequest(page: Int, query: String, filters: FilterList): Request {
        val url = "$baseUrl/api/public/series/search".toHttpUrl().newBuilder().apply {
            addQueryParameter("status", "approved")
            addQueryParameter("limit", "18")
            addQueryParameter("page", page.toString())
            query.takeIf(String::isNotBlank)?.also { addQueryParameter("search", it) }
            filters.firstInstance<TypeFilter>().selected?.also { addQueryParameter("type", it) }
            addQueryParameter("sort", filters.firstInstance<SortFilter>().selected)
            filters.firstInstance<YearFilter>().selected?.also { addQueryParameter("year", it) }
        }.build()
        return GET(url, headers)
    }

    override fun getFilterList() = FilterList(
        TypeFilter(), SortFilter(), YearFilter(), StatusFilter(), GenreFilter(), TagFilter(),
    )

    // =================================================================
    // MANGA DETAILS
    // =================================================================
    override fun mangaDetailsRequest(manga: SManga): Request = GET(getMangaUrl(manga), rscHeaders)
    override fun getMangaUrl(manga: SManga): String = "$baseUrl${manga.url}"

    override fun mangaDetailsParse(response: Response): SManga {
        if (!response.isSuccessful) {
            if (response.code == 403) {
                throw Exception("⚠️ HTTP 403 - تم حظر الوصول\n\n🔧 الحل: افتح WebView من إعدادات الامتداد، تصفح هذه السلسلة يدوياً، ثم ارجع واسحب لأسفل لتحديث التفاصيل.")
            }
            throw Exception("HTTP ${response.code}")
        }

        val manga = response.extractNextJs<Series>()?.series
            ?: throw Exception(
                "⚠️ تعذّر قراءة بيانات السلسلة من الصفحة\n\n" +
                    "🔧 الحل: افتح WebView من إعدادات الامتداد، تصفح هذه السلسلة يدوياً حتى تتأكد " +
                    "من ظهورها بشكل صحيح في المتصفح، ثم ارجع واسحب لأسفل لتحديث التفاصيل.\n" +
                    "(قد يكون الموقع غيّر بنية الصفحة، أو أن هذه السلسلة تحديداً غير متزامنة بين النطاقين)",
            )
        return SManga.create().apply {
            url = "/series/${manga.type}/${manga.id}/${manga.slug}"
            title = manga.title
            artist = manga.metadata.artist.joinToString()
            author = manga.metadata.author.joinToString()
            description = buildString {
                manga.description?.also { append(it.trim()) }
                append("\n\n")
                val altTitles = buildList {
                    addAll(manga.metadata.altTitles)
                    manga.metadata.originalTitle?.also { add(it) }
                }
                if (altTitles.isNotEmpty()) {
                    appendLine("عناوين بديلة")
                    altTitles.forEach { appendLine("- $it") }
                    appendLine()
                }
            }.trim()
            genre = buildList {
                add(manga.type.replaceFirstChar { if (it.isLowerCase()) it.titlecase(Locale.ROOT) else it.toString() })
                manga.metadata.year?.also { add(it) }
                manga.metadata.origin?.also { origin ->
                    add(origin.replaceFirstChar { if (it.isLowerCase()) it.titlecase(Locale.ROOT) else it.toString() })
                }
                when (manga.type) {
                    "manga" -> add("مانجا")
                    "manhwa" -> add("مانها")
                    "manhua" -> add("مانهوا")
                }
                if (manga.metadata.genres.isNotEmpty()) {
                    val genreMap = genres.associate { it.second to it.first }
                    manga.metadata.genres.mapTo(this) { genreMap[it] ?: it }
                }
                if (manga.metadata.tags.isNotEmpty()) {
                    val tagsMap = tags.associate { it.second to it.first }
                    manga.metadata.tags.mapTo(this) { tagsMap[it] ?: it }
                }
            }.joinToString()
            status = when (manga.progress?.trim()) {
                "مستمر" -> SManga.ONGOING
                "مكتمل" -> SManga.COMPLETED
                "متوقف" -> SManga.ON_HIATUS
                else -> SManga.UNKNOWN
            }
            thumbnail_url = (manga.coverImageApp?.desktop ?: manga.metadata.coverImage)?.let {
                if (it.startsWith("/")) manga.cdn?.let { cdn -> "https://$cdn.$cdnDomain$it" } else it
            }
            initialized = true
        }
    }

    // =================================================================
    // CHAPTER LIST
    // =================================================================
    override fun chapterListRequest(manga: SManga) = GET(getMangaUrl(manga), rscHeaders)

    override fun chapterListParse(response: Response): List<SChapter> {
        if (!response.isSuccessful) {
            if (response.code == 403) {
                throw Exception("⚠️ HTTP 403 - تم حظر الوصول\n\n🔧 الحل: افتح WebView، تصفح هذه السلسلة، ثم ارجع واسحب لأسفل لتحديث قائمة الفصول.")
            }
            throw Exception("HTTP ${response.code}")
        }

        // مهم جداً: لا نستخرج type/id/slug من مقاطع رابط الاستجابة (response.request.url)
        // لأن الموقع أصبح يحوّل (301/302 redirect) أي رابط بلا بادئة لغة تلقائياً إلى
        // نسخة ببادئة لغة (مثل procomic.pro/series/... → procomic.pro/ar/series/...)
        // حسب Accept-Language، وOkHttp يتبع هذا التحويل تلقائياً بشكل شفّاف. هذا يجعل
        // الرابط النهائي يحتوي مقطعاً إضافياً ("ar")، فيُزيح كل الفهارس بخانة واحدة
        // ويُفسد بناء رابط ترقيم الفصول بالكامل (وهو سبب خطأ "فشل جلب الصفحة 2 - 404").
        // الحل: نأخذ type/id/slug مباشرة من بيانات JSON نفسها (Series)، وهي غير متأثرة
        // بهذا التحويل مطلقاً بخلاف رابط الطلب.
        val html = response.body.string()
        val htmlMediaType = "text/html; charset=utf-8".toMediaType()
        fun freshHtmlResponse() = response.newBuilder()
            .body(html.toResponseBody(htmlMediaType))
            .build()

        val seriesInfo = freshHtmlResponse().extractNextJs<Series>()?.series
            ?: throw Exception(
                "⚠️ تعذّر قراءة بيانات السلسلة عند جلب الفصول\n\n" +
                    "🔧 الحل: افتح WebView، تصفح هذه السلسلة يدوياً، ثم ارجع واسحب لأسفل لتحديث قائمة الفصول.\n" +
                    "(قد يكون الموقع غيّر بنية الصفحة، أو أن هذه السلسلة تحديداً غير متزامنة بين النطاقين)",
            )
        val data = freshHtmlResponse().extractNextJs<InitialChapters>()
            ?: throw Exception(
                "⚠️ تعذّر قراءة قائمة الفصول الأولية من الصفحة\n\n" +
                    "🔧 الحل: افتح WebView، تصفح هذه السلسلة يدوياً، ثم ارجع واسحب لأسفل لتحديث قائمة الفصول.",
            )
        val chapters = data.initialChapters.toMutableList()
        val size = chapters.size
        var page = 2
        val type = seriesInfo.type
        val id = seriesInfo.id.toString()
        val slug = seriesInfo.slug

        while (data.totalChapters > chapters.size) {
            val requestUrl = "$baseUrl/api/public/$type/$id/chapters?page=${page++}&limit=$size&order=desc"
            val request = GET(requestUrl, headers)
            val nextResponse = client.newCall(request).execute()
            if (!nextResponse.isSuccessful) {
                val code = nextResponse.code
                nextResponse.close()
                if (code == 403) {
                    throw Exception("⚠️ HTTP 403 عند جلب الفصول\n\n🔧 الحل: افتح WebView وتصفح الموقع ثم أعد المحاولة.")
                }
                throw Exception("HTTP $code - فشل جلب الصفحة ${page - 1}\nالرابط: $requestUrl")
            }
            val nextChapters = nextResponse.parseAs<Data<List<Chapter>>>()
            chapters.addAll(nextChapters.data)
        }

        countViews(id)
        return chapters
            .filter { it.language == "AR" }
            .map { chapter ->
                SChapter.create().apply {
                    url = "/series/$type/$id/$slug/${chapter.id}/${chapter.number}"
                    name = buildString {
                        append("\u200F")
                        if (chapter.coins != null && chapter.coins > 0) append("\uD83D\uDD12 ")
                        append("الفصل ")
                        append(chapter.number.toFloat().toString().substringBefore(".0"))
                        chapter.title?.trim()?.takeIf { it.isNotBlank() }?.let { trimmedTitle ->
                            if (trimmedTitle != chapter.number.trim() && trimmedTitle != chapter.number) {
                                append(" \u200F- ")
                                append(trimmedTitle)
                            }
                        }
                    }
                    scanlator = chapter.uploader ?: "\u200B"
                    chapter_number = chapter.number.toFloat()
                    date_upload = dateFormat.tryParse(chapter.createdAt)
                }
            }
            .sortedByDescending { it.chapter_number }
    }

    private val dateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT).apply {
        timeZone = TimeZone.getTimeZone("UTC")
    }

    // =================================================================
    // PAGE LIST
    // =================================================================
    override fun pageListRequest(chapter: SChapter): Request = GET(getChapterUrl(chapter), headers)

    override fun getChapterUrl(chapter: SChapter): String {
        val url = if (chapter.url.startsWith("{")) chapter.url.parseAs<ChapterUrl>() else chapter.url
        return "$baseUrl$url"
    }

    override fun fetchPageList(chapter: SChapter): Observable<List<Page>> {
        return Observable.fromCallable {
            // مهم جداً: لا نمرر ترويسة Cookie يدوياً هنا. عند تعيين ترويسة Cookie
            // صراحةً على الطلب، يتجاهل OkHttp تماماً الكوكيز المخزّنة في الـ CookieJar
            // الخاص بالـ client (وهو ما يحتفظ به `network.cloudflareClient` بعد حل تحدي
            // Cloudflare تلقائياً). قراءة الكوكيز من android.webkit.CookieManager بدلاً
            // من ذلك تعتمد على أن يكون المستخدم قد فتح WebView يدوياً من قبل، وإن لم يكن
            // قد فعل فستُرسل ترويسة Cookie فارغة "" فتُلغي أي كوكيز صالحة كان الـ client
            // نفسه قد حصل عليها. لهذا كانت الصور المؤجلة تفشل دائماً إلا بعد فتح WebView يدوياً.
            val request = pageListRequest(chapter)

            // نستخرج seriesId/chapterId من رابط الطلب *الأصلي* قبل إرساله، لا من رابط
            // الاستجابة النهائي بعد أي تحويل لغة محتمل (/ar/ أو /en/) — لنفس السبب
            // الذي عالجناه في chapterListParse أعلاه.
            val requestSegments = request.url.pathSegments
            val seriesId = requestSegments.getOrNull(2) ?: ""
            val chapterId = requestSegments.getOrNull(4) ?: ""

            val response = client.newCall(request).execute()
            if (!response.isSuccessful) {
                response.close()
                if (response.code == 403) {
                    throw Exception("⚠️ HTTP 403 - فشل جلب الفصل\n\n🔧 الحل:\n1. افتح WebView من إعدادات الامتداد\n2. اذهب إلى هذا الفصل يدوياً وتصفحه حتى تظهر الصور\n3. ارجع واسحب قائمة الفصول لأسفل للتحديث\n4. ثم حاول مرة أخرى.")
                }
                throw Exception("HTTP ${response.code} - فشل جلب صفحات الفصل")
            }
            parsePageList(response, seriesId, chapterId)
        }
    }

    // =================================================================
    // استخراج صفحات الفصل من الـ HTML مباشرة (لا حاجة لأي API إضافي)
    // =================================================================
    // بعد فحص المصدر الفعلي لصفحة فصل حقيقية، تبيّن أن الموقع لا يستخدم إطلاقاً أي
    // "deferredToken" ولا "chapter-deferred-media" ولا JWT ولا خرائط JSON مشفّرة بـ
    // AES كما كان مفترضاً سابقاً (بحثنا في المصدر الحقيقي كاملاً ولم نجد أياً من هذه
    // النصوص إطلاقاً). الصفحات كلها موجودة فعلياً وبشكل كامل داخل HTML الأولي نفسه،
    // على شكلين فقط:
    //
    // 1) صفحة عادية (غير مقسّمة): <div class="leading-[0]"><img alt="page N" ...
    //    src="...mobile.avif"><img alt="page N" ... src="...desktop.avif"></div>
    //    نأخذ نسخة "desktop" لأنها الأعلى جودة.
    //
    // 2) صفحة "مقسّمة بصرياً" لأغراض مكافحة السحب المباشر (وليست مشفّرة إطلاقاً):
    //    <div style="position:relative;...;padding-bottom:X%"> تحتوي عدة <img> بلا
    //    alt، كل واحدة منها لها style="left:L%;top:T%;width:W%;height:H%" يحدد
    //    مكانها الحقيقي والنهائي داخل الصورة الكاملة، بالإضافة إلى <canvas
    //    width="W" height="H"> يعطينا الأبعاد الفعلية بالبكسل مباشرة. أي أن كل ما
    //    نحتاجه لإعادة بناء الصورة الكاملة (روابط القطع + مواقعها الدقيقة) موجود
    //    مسبقاً في الصفحة نفسها، دون أي تشفير أو ترتيب عشوائي يحتاج فك تشفير.
    private val simplePageBlockRegex = Regex(
        """<div class="leading-\[0\]">((?:<img[^>]*?>)+?)</div>""",
    )
    private val tiledPageBlockRegex = Regex(
        """<div style="position:\s*relative;\s*width:\s*100%;\s*padding-bottom:\s*[0-9.]+%;">((?:<img[^>]*?>)+?)<canvas[^>]*?\swidth="(\d+)"[^>]*?\sheight="(\d+)"[^>]*?></canvas></div>""",
    )
    private val imgSrcRegex = Regex("""<img[^>]*?\ssrc="([^"]+)"""")
    private val tilePieceRegex = Regex(
        """<img[^>]*?\ssrc="([^"]+)"[^>]*?\sstyle="left:\s*([0-9.]+)%;\s*top:\s*([0-9.]+)%;\s*width:\s*([0-9.]+)%;\s*height:\s*([0-9.]+)%;"""",
    )

    private fun unescapeHtmlUrl(url: String) = url.replace("&amp;", "&").replace("\\/", "/")

    /** تمثيل خام لصفحة واحدة قبل ترميزها لـ [Page]، يُستخدم أيضاً عند إعادة تحديث التوكنات المنتهية. */
    private sealed class RawPageUnit {
        data class Simple(val imageUrl: String) : RawPageUnit()
        data class Tiled(val canvasWidth: Int, val canvasHeight: Int, val pieces: List<TiledPiece>) : RawPageUnit()
    }

    /** يستخرج كل وحدات الصفحة الخام بالترتيب الذي تظهر فيه في الـ HTML، بلا أي ترميز. */
    private fun extractRawPageUnits(html: String): List<RawPageUnit> {
        data class Positioned(val start: Int, val unit: RawPageUnit?)
        val units = mutableListOf<Positioned>()

        simplePageBlockRegex.findAll(html).forEach { match ->
            val content = match.groupValues[1]
            val srcs = imgSrcRegex.findAll(content).map { unescapeHtmlUrl(it.groupValues[1]) }.toList()
            val chosen = srcs.firstOrNull { "desktop" in it } ?: srcs.firstOrNull()
            units.add(Positioned(match.range.first, chosen?.let { RawPageUnit.Simple(it) }))
        }

        tiledPageBlockRegex.findAll(html).forEach { match ->
            val content = match.groupValues[1]
            val canvasWidth = match.groupValues[2].toIntOrNull()
            val canvasHeight = match.groupValues[3].toIntOrNull()
            if (canvasWidth == null || canvasHeight == null) return@forEach

            val pieces = tilePieceRegex.findAll(content).map { m ->
                TiledPiece(
                    url = unescapeHtmlUrl(m.groupValues[1]),
                    left = m.groupValues[2].toDouble(),
                    top = m.groupValues[3].toDouble(),
                    width = m.groupValues[4].toDouble(),
                    height = m.groupValues[5].toDouble(),
                )
            }.toList()

            val unit = if (pieces.isNotEmpty()) RawPageUnit.Tiled(canvasWidth, canvasHeight, pieces) else null
            units.add(Positioned(match.range.first, unit))
        }

        return units.sortedBy { it.start }.mapNotNull { it.unit }
    }

    /**
     * يمرّ على مقاطع الـ HTML بالترتيب الذي تظهر فيه (صفحات عادية أو مقسّمة)
     * ويبني قائمة [Page] كاملة ومرتّبة دون أي طلب شبكة إضافي.
     *
     * مهم: روابط قطع الصفحات المقسّمة (tiled) موقّعة برمز "token" ووقت انتهاء "expires"
     * قصير الأمد صادر عن الخادم لحظة توليد هذا الـ HTML. إن مرّ وقت كافٍ بين فتح الفصل
     * وقراءة صفحاته الأخيرة فعلياً (قراءة طويلة، أو تحميل مسبق للفصل)، تنتهي صلاحية هذه
     * الروابط، ويستجيب الخادم عندها بصورة بديلة/عامة بدل الصورة الحقيقية — وهو بالضبط
     * سبب ظهور "صورة مكررة بدل تكملة الفصل" التي أبلغ عنها المستخدم. لهذا السبب نحفظ
     * chapterUrl و pageIndex داخل كل [TiledPage] مرمّزة، ليتمكن تحميل الصورة لاحقاً (في
     * tiledImageInterceptor) من اكتشاف التوكنات المنتهية وإعادة جلب رابط جديد صالح لنفس
     * الصفحة تحديداً بدل استخدام الرابط القديم المنتهي.
     */
    private fun extractChapterPagesFromHtml(html: String, chapterUrl: String): List<Page> {
        return extractRawPageUnits(html).mapIndexed { pageIndex, unit ->
            val imageUrl = when (unit) {
                // مهم: الصفحات العادية تحمل أيضاً expires/token قصيري الأجل تماماً كقطع
                // الصفحات المقسّمة (تأكدنا من هذا بفحص عيّنة حقيقية)، فنغلّفها بنفس أسلوب
                // الرابط الداخلي الوهمي حاملةً chapterUrl/pageIndex، لنتمكن من تحديث
                // التوكن المنتهي لاحقاً بدل تمرير الرابط الخام مباشرة كما كان يحدث سابقاً
                // (وهو ما كان يُفسد ترتيب/محتوى الفصل بصورة بديلة/مكررة عند انتهاء التوكن).
                is RawPageUnit.Simple -> encodeSimplePage(SimplePage(unit.imageUrl, chapterUrl, pageIndex))
                is RawPageUnit.Tiled -> encodeTiledPage(
                    TiledPage(unit.canvasWidth, unit.canvasHeight, unit.pieces, chapterUrl, pageIndex),
                )
            }
            Page(pageIndex, chapterUrl, imageUrl)
        }
    }

    private fun encodeTiledPage(page: TiledPage): String {
        val encoded = Base64.encodeToString(
            json.encodeToString(TiledPage.serializer(), page).toByteArray(Charsets.UTF_8),
            Base64.URL_SAFE or Base64.NO_WRAP,
        )
        return "$TILED_SCHEME$encoded"
    }

    private fun encodeSimplePage(page: SimplePage): String {
        val encoded = Base64.encodeToString(
            json.encodeToString(SimplePage.serializer(), page).toByteArray(Charsets.UTF_8),
            Base64.URL_SAFE or Base64.NO_WRAP,
        )
        return "$SIMPLE_SCHEME$encoded"
    }

    private fun extractLazyImagesFromHtml(html: String): List<String> {
        val imageUrls = mutableSetOf<String>()
        val imgRegex = Regex("<img[^>]+(data-(?:lazy-)?src)=(?:'|\")([^'\"]+)(?:'|\")", RegexOption.IGNORE_CASE)
        imgRegex.findAll(html).forEach { match ->
            var url = match.groupValues[2]
            if (url.startsWith("//")) url = "https:$url"
            if (url.startsWith("http")) imageUrls.add(url)
        }
        return imageUrls.toList()
    }

    private fun extractImagesFromJavaScript(html: String): List<String> {
        val scriptRegex = Regex("<script[^>]*>([\\s\\S]*?)</script>", RegexOption.IGNORE_CASE)
        val urlRegex = Regex("""["'](https?://[^"']+\.(?:jpg|jpeg|png|webp|avif|gif)[^"']*)["']""", RegexOption.IGNORE_CASE)
        val allMatches = mutableSetOf<String>()
        
        scriptRegex.findAll(html).forEach { scriptMatch ->
            val scriptContent = scriptMatch.groupValues[1]
            urlRegex.findAll(scriptContent).forEach { urlMatch ->
                var url = urlMatch.groupValues[1].replace("\\/", "/")
                if (url.startsWith("//")) url = "https:$url"
                if (url.startsWith("http")) allMatches.add(url)
            }
        }
        return allMatches.toList()
    }

    override fun pageListParse(response: Response): List<Page> {
        // احتياطي فقط: هذا المسار قد يُستدعى مباشرة من إطار العمل خارج fetchPageList
        // الذي نتحكم به بالكامل أعلاه، فنحاول أفضل استخراج ممكن من رابط الاستجابة.
        val segments = response.request.url.pathSegments
        return parsePageList(response, segments.getOrNull(2) ?: "", segments.getOrNull(4) ?: "")
    }

    private fun parsePageList(response: Response, seriesId: String, chapterId: String): List<Page> {
        val html = response.body.string()
        val chapterUrl = response.request.url.toString()

        val pages = extractChapterPagesFromHtml(html, chapterUrl)

        val finalPages = pages.ifEmpty {
            // احتياطي أخير فقط: إن لم نجد أي صفحة بالطريقة المباشرة أعلاه (مثلاً فصل
            // مقفل بعملات أو تغيّر هيكل الموقع مستقبلاً)، نجرّب المسارات القديمة.
            // هذا المسار غير مؤكد وجوده فعلياً في الموقع (لم نجد له أي أثر في العينات
            // الحقيقية التي تم فحصها)، لذا هو مجرد شبكة أمان ولا يجب الاعتماد عليه.
            legacyFallbackPages(html, chapterUrl, chapterId)
        }

        countViews(seriesId, chapterId)
        return finalPages
    }

    private fun legacyFallbackPages(html: String, chapterUrl: String, chapterId: String): List<Page> {
        val allImageUrls = extractAllImageUrls(html).toMutableSet()
        allImageUrls.addAll(extractLazyImagesFromHtml(html))
        allImageUrls.addAll(extractImagesFromJavaScript(html))
        val embeddedMaps = extractEmbeddedMaps(html)
        val deferredToken = extractDeferredToken(html)

        val pages = mutableListOf<Page>()
        val existingUrls = mutableSetOf<String>()
        var index = 0

        allImageUrls.forEach { url ->
            if (existingUrls.add(url)) {
                pages.add(Page(index++, chapterUrl, url))
            }
        }

        embeddedMaps.forEach { map ->
            if (map.pieces.isNotEmpty()) {
                val encoded = encodeMap(map)
                if (existingUrls.add(map.pieces.first())) {
                    pages.add(Page(index++, chapterUrl, encoded))
                }
            }
        }

        if (deferredToken != null) {
            val apiHeaders = headers.newBuilder()
                .set("Accept", "application/json")
                .set("Referer", chapterUrl)
                .set("Origin", baseUrl)
                .set("X-Requested-With", "XMLHttpRequest")
                .build()

            try {
                var deferredResponse = client.newCall(
                    GET("$baseUrl/chapter-deferred-media/$chapterId?token=$deferredToken", apiHeaders)
                ).execute()

                if (deferredResponse.code == 404) {
                    deferredResponse.close()
                    deferredResponse = client.newCall(
                        GET("$baseUrl/api/public/chapter/$chapterId/deferred-media?token=$deferredToken", apiHeaders)
                    ).execute()
                }

                if (deferredResponse.isSuccessful) {
                    val bodyString = deferredResponse.body.string()
                    deferredResponse.close()

                    try {
                        val deferredData = json.decodeFromString<Data<DeferredImages>>(bodyString)
                        deferredData.data.images.forEach { url ->
                            if (existingUrls.add(url)) {
                                pages.add(Page(index++, chapterUrl, url))
                            }
                        }
                        deferredData.data.maps.forEach { scrambledData ->
                            val map = when (scrambledData) {
                                is ScrambledImage -> ScrambledMap(
                                    dim = scrambledData.dim,
                                    mode = scrambledData.mode,
                                    pieces = scrambledData.pieces,
                                    order = scrambledData.order
                                )
                                is ScrambledImageToken -> {
                                    val decoded = decodeScrambledImageToken(scrambledData)
                                    ScrambledMap(
                                        dim = decoded.dim,
                                        mode = decoded.mode,
                                        pieces = decoded.pieces,
                                        order = decoded.order
                                    )
                                }
                            }
                            val key = map.pieces.firstOrNull() ?: return@forEach
                            if (existingUrls.add(key)) {
                                pages.add(Page(index++, chapterUrl, encodeMap(map)))
                            }
                        }
                    } catch (e: Exception) {
                        try {
                            val chapterDeferred = json.decodeFromString<ChapterDeferredResponse>(bodyString)
                            if (chapterDeferred.success && chapterDeferred.data != null) {
                                chapterDeferred.data.images.forEach { url ->
                                    if (existingUrls.add(url)) {
                                        pages.add(Page(index++, chapterUrl, url))
                                    }
                                }
                                chapterDeferred.data.maps.forEach { map ->
                                    val key = map.pieces.firstOrNull() ?: return@forEach
                                    if (existingUrls.add(key)) {
                                        pages.add(Page(index++, chapterUrl, encodeMap(map)))
                                    }
                                }
                            }
                        } catch (e2: Exception) {
                            // لا نرمي استثناءً هنا: هذا المسار احتياطي غير مؤكد أصلاً.
                        }
                    }
                } else {
                    deferredResponse.close()
                }
            } catch (e: Exception) {
                Log.e(name, "فشل الاتصال بالصور المؤجلة (مسار احتياطي)", e)
            }
        }

        if (pages.isEmpty()) {
            throw Exception("⚠️ لم يتم العثور على أي صفحات لهذا الفصل.\n\n🔧 الحل: افتح WebView من إعدادات الامتداد، تصفح هذا الفصل يدوياً، ثم أعد المحاولة، وإن استمرت المشكلة فقد يكون هيكل الموقع قد تغيّر.")
        }

        return pages
    }

    private fun extractAllImageUrls(html: String): List<String> {
        val urls = mutableSetOf<String>()
        val imagesBlockRegex = Regex("\"images\"\\s*:\\s*\\[(.*?)\\]", RegexOption.DOT_MATCHES_ALL)
        val match = imagesBlockRegex.find(html) ?: return emptyList()
        val blockContent = match.groupValues[1]
        val urlRegex = Regex("\"((https?:)?//[^\"]+)\"")
        urlRegex.findAll(blockContent).forEach {
            var url = it.groupValues[1]
            if (url.startsWith("//")) url = "https:$url"
            if (url.startsWith("http") && (url.contains("/media/") || url.endsWith(".avif") || url.endsWith(".webp") ||
                url.endsWith(".jpg") || url.endsWith(".jpeg") || url.endsWith(".png"))) {
                urls.add(url)
            }
        }
        return urls.toList()
    }

    private fun extractEmbeddedMaps(html: String): List<ScrambledMap> {
        return try {
            val mapsRegex = Regex("\"maps\"\\s*:\\s*\\[(\\{.*?\\})\\]", RegexOption.DOT_MATCHES_ALL)
            val match = mapsRegex.find(html) ?: return emptyList()
            val mapsJson = "[${match.groupValues[1]}]"
            json.decodeFromString<List<ScrambledMap>>(mapsJson)
        } catch (e: Exception) {
            emptyList()
        }
    }

    private fun extractDeferredToken(html: String): String? {
        val specificTokenRegex = Regex("""\\?"deferredToken\\?"\s*:\s*\\?"([^"\\]+)\\?"""")
        specificTokenRegex.find(html)?.let { return it.groupValues[1] }

        val tokenRegex = Regex("""\\?"token\\?"\s*:\s*\\?"(eyJ[a-zA-Z0-9-_.]+)\\?"""")
        tokenRegex.find(html)?.let { return it.groupValues[1] }

        val jwtRegex = Regex("""eyJ[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+""")
        val matches = jwtRegex.findAll(html).map { it.value }.toList()
        return matches.lastOrNull()
    }

    private fun encodeMap(map: ScrambledMap): String {
        val encoded = Base64.encodeToString(
            json.encodeToString(ScrambledMap.serializer(), map).toByteArray(Charsets.UTF_8),
            Base64.URL_SAFE or Base64.NO_WRAP
        )
        return "$SCRAMBLED_SCHEME$encoded"
    }

    override fun imageRequest(page: Page): Request {
        val headers = headersBuilder()
            .set("Accept", "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8")
            .set("Referer", page.url)
            .set("Sec-Fetch-Dest", "image")
            .set("Sec-Fetch-Mode", "no-cors")
            .set("Sec-Fetch-Site", "same-site")
            .build()
        return GET(page.imageUrl!!, headers)
    }

    // =================================================================
    // TILED IMAGE INTERCEPTOR (الصفحات المقسّمة بصرياً فعلياً في الموقع)
    // =================================================================
    private fun tiledImageInterceptor(chain: Interceptor.Chain): Response {
        val request = chain.request()
        val url = request.url.toString()

        if (!url.startsWith(TILED_SCHEME)) {
            return chain.proceed(request)
        }

        val encoded = url.removePrefix(TILED_SCHEME)
        val pageJson = String(Base64.decode(encoded, Base64.URL_SAFE or Base64.NO_WRAP), Charsets.UTF_8)
        val tiledPage = json.decodeFromString<TiledPage>(pageJson)

        // مهم جداً: نستخدم نفس ترويسات الطلب الأصلي (التي ضبطناها بعناية في imageRequest():
        // Referer + User-Agent + Accept صورة صحيحة) عند تحميل كل قطعة من قطع الصفحة على
        // حدة. سابقاً كانت reconstructTiledPage تستدعي GET(piece.url) بلا أي ترويسات
        // إطلاقاً، فكان خادم CDN (procomic.net) يحمي الصور من hotlinking ويرفض/يتجاهل
        // نسبة كبيرة من طلبات القطع "العارية" هذه بصمت (والكود كان يتجاوزها بـ continue
        // دون أي إعادة محاولة) — وهذا بالضبط سبب فقدان أجزاء كثيرة من كل صفحة مقسّمة.
        val pieceHeaders = request.headers

        val mergedBytes = reconstructTiledPage(tiledPage, pieceHeaders)
            ?: return Response.Builder()
                .request(request)
                .protocol(Protocol.HTTP_1_1)
                .code(500)
                .message("فشل دمج الصورة المقسّمة")
                .body("".toResponseBody(null))
                .build()

        return Response.Builder()
            .request(request)
            .protocol(Protocol.HTTP_1_1)
            .code(200)
            .message("OK")
            .body(mergedBytes.toResponseBody("image/jpeg".toMediaType()))
            .build()
    }

    /**
     * يعيد بناء الصفحة الكاملة من قطعها بالاعتماد على النسب المئوية الحقيقية
     * (left/top/width/height) وأبعاد الـ canvas بالبكسل، كما هي موجودة فعلياً في HTML
     * الموقع — لا يوجد أي تشفير أو ترتيب عشوائي يحتاج فك تشفير في هذا النوع من الصفحات.
     */
    private fun reconstructTiledPage(originalPage: TiledPage, pieceHeaders: Headers): ByteArray? {
        // إن كانت أي من روابط قطع هذه الصفحة تحمل توكناً أوشك على الانتهاء (أو انتهى
        // فعلاً)، نجلب نسخة جديدة من صفحة الفصل، ونستخرج توكنات طازجة لنفس هذه الصفحة
        // تحديداً (بنفس ترتيبها/فهرسها)، بدل استخدام الروابط القديمة المنتهية التي يردّ
        // عليها الخادم بصورة بديلة/عامة — وهذا بالضبط ما كان يظهر كـ"صورة مكررة".
        val page = refreshTiledPageIfNeeded(originalPage)

        if (page.pieces.isEmpty() || page.canvasWidth <= 0 || page.canvasHeight <= 0) return null

        val result = Bitmap.createBitmap(page.canvasWidth, page.canvasHeight, Bitmap.Config.ARGB_8888)
        val canvas = Canvas(result)
        var drewAny = false

        // نحمّل كل القطع بالتوازي (بدل التتابع) عبر مسبح خيوط صغير: هذا يسرّع إعادة
        // البناء بشكل كبير للصفحات كثيرة القطع، ويمنع أن يؤخّر تعليق قطعة واحدة كل
        // الباقي حتى ينتهي المهلة الزمنية للطلب الأصلي من Mihon/Tachiyomi.
        val pool = Executors.newFixedThreadPool(minOf(page.pieces.size, 6))
        try {
            val futures = page.pieces.map { piece ->
                pool.submit<Bitmap?> { fetchTileBitmap(piece.url, pieceHeaders) }
            }

            for (i in page.pieces.indices) {
                val piece = page.pieces[i]
                val bmp = try {
                    futures[i].get(25, TimeUnit.SECONDS)
                } catch (e: Exception) {
                    Log.e("ProChan-Debug", "انتهت مهلة تحميل قطعة الصورة المقسّمة: ${e.message}")
                    null
                } ?: continue

                val left = (piece.left / 100.0 * page.canvasWidth).toInt()
                val top = (piece.top / 100.0 * page.canvasHeight).toInt()
                val right = left + (piece.width / 100.0 * page.canvasWidth).toInt()
                val bottom = top + (piece.height / 100.0 * page.canvasHeight).toInt()

                canvas.drawBitmap(bmp, null, Rect(left, top, right, bottom), null)
                bmp.recycle()
                drewAny = true
            }
        } finally {
            pool.shutdown()
        }

        if (!drewAny) {
            result.recycle()
            return null
        }

        val out = ByteArrayOutputStream()
        result.compress(Bitmap.CompressFormat.JPEG, 90, out)
        result.recycle()
        return out.toByteArray()
    }

    /**
     * يحمّل قطعة صورة واحدة مع الترويسات الصحيحة (Referer/User-Agent/Accept)، ويعيد
     * المحاولة تلقائياً حتى 3 مرات مع تأخير متزايد عند فشل مؤقت (شبكة/429/5xx)، بدل
     * التخلي عن القطعة من أول فشل كما كان يحدث سابقاً.
     */
    private fun fetchTileBitmap(url: String, pieceHeaders: Headers, attempt: Int = 1): Bitmap? {
        try {
            val resp = client.newCall(GET(url, pieceHeaders)).execute()
            if (!resp.isSuccessful) {
                val code = resp.code
                resp.close()
                if (attempt < 3) {
                    Thread.sleep(300L * attempt)
                    return fetchTileBitmap(url, pieceHeaders, attempt + 1)
                }
                Log.e("ProChan-Debug", "فشل تحميل قطعة صورة نهائياً بعد 3 محاولات (HTTP $code): $url")
                return null
            }
            val bytes = resp.body.bytes()
            resp.close()
            return decodeAvif(bytes)
        } catch (e: Exception) {
            if (attempt < 3) {
                Thread.sleep(300L * attempt)
                return fetchTileBitmap(url, pieceHeaders, attempt + 1)
            }
            Log.e("ProChan-Debug", "فشل تحميل قطعة صورة نهائياً بعد 3 محاولات: ${e.message}")
            return null
        }
    }

    // =================================================================
    // تحديث التوكنات المنتهية (سبب "الصورة المكررة")
    // =================================================================
    // كل رابط قطعة صورة مقسّمة يحمل معامِلَي "expires" (وقت انتهاء بالثواني منذ
    // Epoch) و"token" (توقيع). هذه التوكنات صادرة لحظة توليد HTML الفصل، وتنتهي
    // صلاحيتها بعد فترة قصيرة. إن استغرقت القراءة الفعلية للفصل وقتاً أطول من ذلك
    // (فصل طويل، أو قراءة متأخرة عن وقت فتحه)، يرفض الخادم الرابط المنتهي ويردّ بصورة
    // بديلة/عامة بدل الصورة الحقيقية — فتظهر نفس الصورة مكررة بدل تكملة الفصل.
    private val expiresParamRegex = Regex("""[?&]expires=(\d+)""")

    /** نعتبر التوكن على وشك الانتهاء إن تبقّى له أقل من هامش أمان معيّن، لضمان اكتمال التحميل قبل الرفض الفعلي. */
    private fun isTokenExpiringSoon(url: String, safetyMarginSeconds: Long = 30L): Boolean {
        val expiresAt = expiresParamRegex.find(url)?.groupValues?.get(1)?.toLongOrNull() ?: return false
        val nowSeconds = System.currentTimeMillis() / 1000
        return expiresAt - nowSeconds < safetyMarginSeconds
    }

    // تخزين مؤقت قصير الأمد لوحدات صفحات الفصل الطازجة، مفتاحه رابط الفصل، لتفادي
    // إعادة جلب وتحليل HTML الفصل كاملاً من جديد لكل صفحة على حدة إن احتاجت عدة صفحات
    // متتالية تحديثاً في نفس الفترة الزمنية القصيرة (كما يحدث عادة أثناء القراءة).
    private val freshPageUnitsCache = ConcurrentHashMap<String, Pair<Long, List<RawPageUnit>>>()
    private val freshUnitsCacheTtlMillis = 60_000L

    private fun fetchFreshPageUnits(chapterUrl: String): List<RawPageUnit>? {
        val now = System.currentTimeMillis()
        freshPageUnitsCache[chapterUrl]?.let { (ts, units) ->
            if (now - ts < freshUnitsCacheTtlMillis) return units
        }

        return try {
            val resp = client.newCall(GET(chapterUrl, headers)).execute()
            if (!resp.isSuccessful) {
                resp.close()
                return null
            }
            val html = resp.body.string()
            resp.close()
            val units = extractRawPageUnits(html)
            freshPageUnitsCache[chapterUrl] = now to units
            units
        } catch (e: Exception) {
            Log.e("ProChan-Debug", "فشل تحديث توكنات الفصل (إعادة جلب الفصل): ${e.message}")
            null
        }
    }

    /** يعيد نفس الصفحة إن كانت توكناتها ما زالت صالحة، أو نسخة طازجة بتوكنات جديدة لنفس رقم الصفحة إن انتهت صلاحيتها. */
    private fun refreshTiledPageIfNeeded(page: TiledPage): TiledPage {
        if (page.chapterUrl.isEmpty() || page.pageIndex < 0) return page
        val needsRefresh = page.pieces.any { isTokenExpiringSoon(it.url) }
        if (!needsRefresh) return page

        val freshUnits = fetchFreshPageUnits(page.chapterUrl) ?: return page
        val freshUnit = freshUnits.getOrNull(page.pageIndex) as? RawPageUnit.Tiled ?: return page

        return TiledPage(freshUnit.canvasWidth, freshUnit.canvasHeight, freshUnit.pieces, page.chapterUrl, page.pageIndex)
    }

    // =================================================================
    // SIMPLE IMAGE INTERCEPTOR (صفحات غير مقسّمة، لكن تحمل توكن قصير الأجل أيضاً)
    // =================================================================
    // بلا هذا الاعتراض، كان رابط الصورة الخام (مع expires/token) يُرسل كما هو، وإن كان
    // قد انتهى (تأخر تحميل الصفحة عن Mihon قليلاً) كان الخادم يردّ بصورة بديلة/عامة —
    // وهذا بالضبط سبب "صورة مكررة تظهر عشوائياً" و"الفصل يبدأ بصورة ليست البداية"، لأن
    // أول صفحات الفصل أحياناً من هذا النوع (غير مقسّم) وتوكنها ينتهي أول ما ينتهي.
    private fun simpleImageInterceptor(chain: Interceptor.Chain): Response {
        val request = chain.request()
        val url = request.url.toString()

        if (!url.startsWith(SIMPLE_SCHEME)) {
            return chain.proceed(request)
        }

        val encoded = url.removePrefix(SIMPLE_SCHEME)
        val pageJson = String(Base64.decode(encoded, Base64.URL_SAFE or Base64.NO_WRAP), Charsets.UTF_8)
        val simplePage = json.decodeFromString<SimplePage>(pageJson)

        val freshImageUrl = refreshSimpleUrlIfNeeded(simplePage)
        val realRequest = request.newBuilder().url(freshImageUrl).build()
        return chain.proceed(realRequest)
    }

    /** يعيد نفس رابط الصورة إن كان توكنه ما زال صالحاً، أو رابطاً طازجاً لنفس رقم الصفحة إن أوشك على الانتهاء. */
    private fun refreshSimpleUrlIfNeeded(page: SimplePage): String {
        if (!isTokenExpiringSoon(page.imageUrl)) return page.imageUrl
        if (page.chapterUrl.isEmpty() || page.pageIndex < 0) return page.imageUrl

        val freshUnits = fetchFreshPageUnits(page.chapterUrl) ?: return page.imageUrl
        val freshUnit = freshUnits.getOrNull(page.pageIndex) as? RawPageUnit.Simple ?: return page.imageUrl
        return freshUnit.imageUrl
    }

    // =================================================================
    // SCRAMBLED IMAGE INTERCEPTOR
    // =================================================================
    private fun scrambledImageInterceptor(chain: Interceptor.Chain): Response {
        val request = chain.request()
        val url = request.url.toString()

        if (!url.startsWith(SCRAMBLED_SCHEME)) {
            return chain.proceed(request)
        }

        val encoded = url.removePrefix(SCRAMBLED_SCHEME)
        val mapJson = String(Base64.decode(encoded, Base64.URL_SAFE or Base64.NO_WRAP), Charsets.UTF_8)
        val map = json.decodeFromString<ScrambledMap>(mapJson)

        // نفس إصلاح الصفحات المقسّمة أعلاه: نستخدم ترويسات الطلب الأصلي الصحيحة بدل
        // طلبات "عارية" بلا Referer/User-Agent كانت تُفقد بصمت جزءاً من القطع.
        val pieceHeaders = request.headers

        val mergedBytes = reconstructPage(map, pieceHeaders)
            ?: return Response.Builder()
                .request(request)
                .protocol(Protocol.HTTP_1_1)
                .code(500)
                .message("فشل دمج الصورة")
                .body("".toResponseBody(null))
                .build()

        return Response.Builder()
            .request(request)
            .protocol(Protocol.HTTP_1_1)
            .code(200)
            .message("OK")
            .body(mergedBytes.toResponseBody("image/jpeg".toMediaType()))
            .build()
    }

    private fun reconstructPage(map: ScrambledMap, pieceHeaders: Headers): ByteArray? {
        val totalW = map.dim.getOrElse(0) { 800 }
        val totalH = map.dim.getOrElse(1) { 1200 }
        val n = map.pieces.size
        if (n == 0) return null

        val rawBitmaps = arrayOfNulls<Bitmap>(n)
        try {
            // تحميل متوازٍ + إعادة محاولة لكل قطعة (نفس منطق الصفحات المقسّمة أعلاه)،
            // بدل التتابع بلا ترويسات ولا إعادة محاولة الذي كان يُفقد قطعاً كثيرة.
            val pool = Executors.newFixedThreadPool(minOf(n, 6))
            try {
                val futures = (0 until n).map { i ->
                    pool.submit<Bitmap?> { fetchTileBitmap(map.pieces[i], pieceHeaders) }
                }
                for (i in 0 until n) {
                    rawBitmaps[i] = try {
                        futures[i].get(25, TimeUnit.SECONDS)
                    } catch (e: Exception) {
                        Log.e("ProChan-Debug", "انتهت مهلة تحميل القطعة $i: ${e.message}")
                        null
                    }
                }
            } finally {
                pool.shutdown()
            }

            val orderedBitmaps = Array(n) { pos ->
                rawBitmaps.getOrNull(map.order.getOrElse(pos) { pos })
            }

            val (cols, rows) = parseMode(map.mode, n)
            val isVertical = map.mode.startsWith("vertical_")

            val result: Bitmap
            val canvas: Canvas

            if (isVertical) {
                val actualH = orderedBitmaps.filterNotNull().sumOf { it.height }
                val canvasH = if (actualH > 0) actualH else totalH
                result = Bitmap.createBitmap(totalW, canvasH, Bitmap.Config.ARGB_8888)
                canvas = Canvas(result)
                var yOffset = 0
                for (pos in 0 until n) {
                    val bmp = orderedBitmaps[pos] ?: continue
                    canvas.drawBitmap(bmp, null, Rect(0, yOffset, totalW, yOffset + bmp.height), null)
                    yOffset += bmp.height
                }
            } else {
                result = Bitmap.createBitmap(totalW, totalH, Bitmap.Config.ARGB_8888)
                canvas = Canvas(result)
                for (pos in 0 until (cols * rows)) {
                    val bmp = orderedBitmaps.getOrNull(pos) ?: continue
                    val col = pos % cols
                    val row = pos / cols
                    val x0 = col * totalW / cols
                    val x1 = if (col == cols - 1) totalW else (col + 1) * totalW / cols
                    val y0 = row * totalH / rows
                    val y1 = if (row == rows - 1) totalH else (row + 1) * totalH / rows
                    canvas.drawBitmap(bmp, null, Rect(x0, y0, x1, y1), null)
                }
            }

            val out = ByteArrayOutputStream()
            result.compress(Bitmap.CompressFormat.JPEG, 90, out)
            result.recycle()
            rawBitmaps.forEach { it?.recycle() }
            return out.toByteArray()
        } catch (e: Exception) {
            rawBitmaps.forEach { it?.recycle() }
            return null
        }
    }

    private fun decodeAvif(bytes: ByteArray): Bitmap? {
        if (bytes.isEmpty()) return null
        try {
            val decoder = ImageDecoder.newInstance(bytes.inputStream())
            if (decoder != null) {
                return try {
                    decoder.decode()
                } catch (e: Exception) {
                    null
                } finally {
                    decoder.recycle()
                }
            }
        } catch (_: Exception) {}
        return try {
            BitmapFactory.decodeByteArray(bytes, 0, bytes.size)
        } catch (_: Exception) {
            null
        }
    }

    private fun parseMode(mode: String, pieceCount: Int): Pair<Int, Int> {
        return when {
            mode.startsWith("grid_") -> {
                val parts = mode.removePrefix("grid_").split("x")
                Pair(
                    parts.getOrNull(0)?.toIntOrNull() ?: 1,
                    parts.getOrNull(1)?.toIntOrNull() ?: 1,
                )
            }
            mode.startsWith("vertical_") -> {
                val count = mode.removePrefix("vertical_").toIntOrNull() ?: pieceCount
                Pair(1, count)
            }
            else -> Pair(1, pieceCount)
        }
    }

    private val sessionKey = ConcurrentHashMap<Int, Pair<String, Long>>()
    private val sessionKeyLock = Any()

    private fun decodeScrambledImageToken(data: ScrambledImageToken): ScrambledImage {
        val value = String(urlSafeBase64(data.token), Charsets.UTF_8)
            .parseAs<ScrambledImageTokenValue>()

        val iv = urlSafeBase64(value.iv)
        val tag = urlSafeBase64(value.tag)
        val encryptedData = urlSafeBase64(value.data)

        val key = when {
            value.m == "browser" && value.v == 2 -> {
                val hash = MessageDigest.getInstance("SHA-256")
                    .digest(
                        "prochan-browser-map:2e6f9a1c4d8b7e3f0a5c9d2b6e1f4a8c7d3b0e6a9f2c5d8b1e4a7c0d3f6b9e2:${value.cid}"
                            .toByteArray(Charsets.UTF_8),
                    )
                SecretKeySpec(hash, "AES")
            }
            value.m == "browser_session" && value.v == 3 -> synchronized(sessionKeyLock) {
                val time = System.currentTimeMillis()
                val key = sessionKey[value.cid]?.takeIf { it.second > time }?.first ?: run {
                    val request = GET("$baseUrl/chapter-map-session-key/${value.cid}", headers)
                    val response = client.newCall(request).execute()
                    if (!response.isSuccessful) {
                        val code = response.code
                        response.close()
                        if (code == 403) {
                            throw Exception("⚠️ HTTP 403 - فشل جلب مفتاح الجلسة\n\n🔧 الحل: افتح WebView وتصفح الموقع ثم أعد المحاولة.")
                        }
                        throw Exception("HTTP $code - فشل جلب مفتاح الصورة المشفرة")
                    }
                    val keyData = response.parseAs<Data<Key>>()
                    sessionKey[value.cid] = keyData.data.key to (time + 120000)
                    keyData.data.key
                }
                SecretKeySpec(urlSafeBase64(key), "AES")
            }
            else -> throw Exception("Unknown method: ${value.m} v${value.v}")
        }

        val cipher = Cipher.getInstance("AES/GCM/NoPadding").apply {
            val spec = GCMParameterSpec(128, iv)
            init(Cipher.DECRYPT_MODE, key, spec)
        }

        val decryptedBytes = cipher.doFinal(encryptedData + tag)
        return String(decryptedBytes, Charsets.UTF_8).parseAs()
    }

    private fun urlSafeBase64(data: String): ByteArray {
        return android.util.Base64.decode(data, android.util.Base64.URL_SAFE)
    }

    private fun countViews(seriesId: String, chapterId: String? = null) {
        val userAgent = headers["User-Agent"]!!
        val payload = ViewsDto(
            chapterId = chapterId?.toInt(),
            contentId = seriesId.toInt(),
            deviceType = when {
                MOBILE_REGEX.containsMatchIn(userAgent) -> "mobile"
                TABLES_REGEX.containsMatchIn(userAgent) -> "tablet"
                else -> "desktop"
            },
            surface = when {
                chapterId == null -> "series"
                else -> "chapter"
            },
        ).toJsonString().toRequestBody(JSON_MEDIA_TYPE)

        client.newCall(POST("$baseUrl/api/views", headers, payload))
            .enqueue(
                object : Callback {
                    override fun onResponse(call: Call, response: Response) {
                        if (!response.isSuccessful) {
                            Log.e(name, "Failed to count views, HTTP ${response.code}")
                        }
                        response.closeQuietly()
                    }
                    override fun onFailure(call: Call, e: IOException) {
                        Log.e(name, "Failed to count views", e)
                    }
                },
            )
    }

    // =================================================================
    // UNSUPPORTED METHODS
    // =================================================================
    override fun popularMangaRequest(page: Int): Request = throw UnsupportedOperationException()
    override fun popularMangaParse(response: Response): MangasPage = throw UnsupportedOperationException()
    override fun latestUpdatesRequest(page: Int): Request = throw UnsupportedOperationException()
    override fun latestUpdatesParse(response: Response): MangasPage = throw UnsupportedOperationException()
    override fun searchMangaParse(response: Response): MangasPage = throw UnsupportedOperationException()
    override fun imageUrlParse(response: Response): String = throw UnsupportedOperationException()
}

private val SUPPORTED_TYPES = setOf("manga", "manhwa", "manhua", "webtoon", "comic")
private val JSON_MEDIA_TYPE = "application/json".toMediaType()
private val MOBILE_REGEX = Regex("mobile|android|iphone|ipad|ipod", RegexOption.IGNORE_CASE)
private val TABLES_REGEX = Regex("tablet", RegexOption.IGNORE_CASE)

/**
 * صفحة عادية (غير مقسّمة) مغلّفة بنفس أسلوب [TiledPage]: تحمل رابط الصورة الخام مع
 * chapterUrl/pageIndex لنفس الفصل والصفحة، لنتمكن من طلب رابط طازج لها إن انتهت
 * صلاحية توكنها (expires/token) قبل أن يصل طلب الصورة الفعلي من Mihon/Tachiyomi.
 */
@Serializable
data class SimplePage(
    val imageUrl: String,
    val chapterUrl: String = "",
    val pageIndex: Int = -1,
)

/**
 * قطعة واحدة من صفحة "مقسّمة بصرياً" كما تُرسم فعلياً بالموقع عبر CSS: نسب مئوية
 * تحدد موقعها (left/top) وحجمها (width/height) داخل حاوية الصفحة الكاملة.
 */
@Serializable
data class TiledPiece(
    val url: String,
    val left: Double,
    val top: Double,
    val width: Double,
    val height: Double,
)

/**
 * صفحة كاملة مبنية من عدة [TiledPiece]، بأبعاد حقيقية بالبكسل (canvasWidth/Height)
 * مأخوذة مباشرة من عنصر <canvas> الموجود في نفس حاوية الصفحة بالـ HTML الأصلي.
 */
@Serializable
data class TiledPage(
    val canvasWidth: Int,
    val canvasHeight: Int,
    val pieces: List<TiledPiece>,
    // اسم الحقلين الجديدين يبدأ بقيمة افتراضية للحفاظ على توافق أي بيانات مرمّزة/محفوظة
    // مسبقاً؛ يُستخدمان فقط لإعادة جلب توكن جديد لنفس الصفحة تحديداً إن انتهت صلاحيته.
    val chapterUrl: String = "",
    val pageIndex: Int = -1,
)
