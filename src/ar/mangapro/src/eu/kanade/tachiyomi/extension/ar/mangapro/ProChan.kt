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
    // نطاق الموقع الأساسي
    private val domain = "procomic.pro"
    private val altDomain = "procomic.net"
    private val cdnDomain = "procomic.net"

    override val baseUrl = "https://$domain"
    override val supportsLatest = true
    override val versionId = 18

    private val json = Json { ignoreUnknownKeys = true }

    companion object {
        private const val SCRAMBLED_SCHEME = "https://procomic.pro/__scrambled__/?map="
        private const val TILED_SCHEME = "https://procomic.pro/__tiled__/?map="
        private const val SIMPLE_SCHEME = "https://procomic.pro/__simple__/?map="
    }

    override val client: OkHttpClient = network.cloudflareClient.newBuilder()
        .addInterceptor(::domainFallbackInterceptor)
        .addInterceptor(::simpleImageInterceptor)
        .addInterceptor(::scrambledImageInterceptor)
        .addInterceptor(::tiledImageInterceptor)
        .addNetworkInterceptor(
            CookieInterceptor(
                domain,
                listOf("safe_browsing" to "off", "language" to "ar")
            )
        )
        .build()

    // =====================================================================
    // Interceptor للتبديل بين النطاقات
    // =====================================================================
    private fun domainFallbackInterceptor(chain: Interceptor.Chain): Response {
        val request = chain.request()
        val path = request.url.encodedPath
        // نستثني الروابط الداخلية الوهمية
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

    // =====================================================================
    // ترويسات HTTP
    // =====================================================================
    override fun headersBuilder(): Headers.Builder {
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

    // =====================================================================
    // البحث والقوائم
    // =====================================================================
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

    // =====================================================================
    // تفاصيل السلسلة
    // =====================================================================
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

    // =====================================================================
    // قائمة الفصول
    // =====================================================================
    override fun chapterListRequest(manga: SManga) = GET(getMangaUrl(manga), rscHeaders)

    override fun chapterListParse(response: Response): List<SChapter> {
        if (!response.isSuccessful) {
            if (response.code == 403) {
                throw Exception("⚠️ HTTP 403 - تم حظر الوصول\n\n🔧 الحل: افتح WebView، تصفح هذه السلسلة، ثم ارجع واسحب لأسفل لتحديث قائمة الفصول.")
            }
            throw Exception("HTTP ${response.code}")
        }

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

    // =====================================================================
    // استخراج صفحات الفصل
    // =====================================================================
    override fun pageListRequest(chapter: SChapter): Request = GET(getChapterUrl(chapter), headers)

    override fun getChapterUrl(chapter: SChapter): String {
        val url = if (chapter.url.startsWith("{")) chapter.url.parseAs<ChapterUrl>() else chapter.url
        return "$baseUrl$url"
    }

    override fun fetchPageList(chapter: SChapter): Observable<List<Page>> {
        return Observable.fromCallable {
            val request = pageListRequest(chapter)
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

    // أنماط regex لاستخراج الصفحات
    private val simplePageBlockRegex = Regex(
        """<div class="leading-\[0\]">((?:<img[^>]*?>)+?)</div>""",
    )
    private val tiledPageBlockRegex = Regex(
        """<div style="position:\s*relative;\s*width:\s*100%;\s*padding-bottom:\s*[0-9.]+%;">((?:<img[^>]*?>)+?)<canvas[^>]*?\swidth="(\d+)"[^>]*?\sheight="(\d+)"[^>]*?></canvas></div>""",
    )
    private val imgSrcRegex = Regex("""<img[^>]*?\ssrc="([^"]+)"""")
    private val tilePieceRegex = Regex(
        """<img[^>]*?\ssrc="([^"]+)"[^>]*?\sstyle="left:\s*([0-9.]+)%;\s*top:\s*([0-9.]+)%;\s*width:\s*([0-9.]+)%;\s*height:\s*([0-9.]+)%;""",
    )

    private fun unescapeHtmlUrl(url: String) = url.replace("&amp;", "&").replace("\\/", "/")

    /** تمثيل خام لصفحة واحدة */
    private sealed class RawPageUnit {
        data class Simple(val imageUrl: String) : RawPageUnit()
        data class Tiled(val canvasWidth: Int, val canvasHeight: Int, val pieces: List<TiledPiece>) : RawPageUnit()
    }

    /** يستخرج كل وحدات الصفحة الخام بالترتيب */
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
     * تحويل الوحدات الخام إلى قائمة Pages.
     * الصفحات البسيطة التي تحتوي على توكنات يتم ترميزها بـ SIMPLE_SCHEME لتحديثها لاحقاً.
     */
    private fun extractChapterPagesFromHtml(html: String, chapterUrl: String): List<Page> {
        return extractRawPageUnits(html).mapIndexed { pageIndex, unit ->
            val imageUrl = when (unit) {
                is RawPageUnit.Simple -> {
                    val url = unit.imageUrl
                    // إذا كان الرابط يحتوي على expires=، نقوم بتشفيره لتحديثه لاحقاً
                    if (url.contains("expires=")) {
                        encodeSimplePage(SimplePage(chapterUrl, pageIndex, url))
                    } else {
                        url
                    }
                }
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

    override fun pageListParse(response: Response): List<Page> {
        val segments = response.request.url.pathSegments
        return parsePageList(response, segments.getOrNull(2) ?: "", segments.getOrNull(4) ?: "")
    }

    private fun parsePageList(response: Response, seriesId: String, chapterId: String): List<Page> {
        val html = response.body.string()
        val chapterUrl = response.request.url.toString()

        val pages = extractChapterPagesFromHtml(html, chapterUrl)

        val finalPages = pages.ifEmpty {
            // مسار احتياطي قديم (نادراً ما يستخدم)
            legacyFallbackPages(html, chapterUrl, chapterId)
        }

        countViews(seriesId, chapterId)
        return finalPages
    }

    // =====================================================================
    // المسار الاحتياطي القديم (قد لا يعمل)
    // =====================================================================
    private fun legacyFallbackPages(html: String, chapterUrl: String, chapterId: String): List<Page> {
        // نسخة مختصرة، في العادة لن نصل هنا
        throw Exception("⚠️ لم يتم العثور على أي صفحات لهذا الفصل.\n\n🔧 الحل: افتح WebView من إعدادات الامتداد، تصفح هذا الفصل يدوياً، ثم أعد المحاولة، وإن استمرت المشكلة فقد يكون هيكل الموقع قد تغيّر.")
    }

    // =====================================================================
    // طلب الصورة
    // =====================================================================
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

    // =====================================================================
    // Simple Image Interceptor (للصور البسيطة ذات التوكنات)
    // =====================================================================
    private fun simpleImageInterceptor(chain: Interceptor.Chain): Response {
        val request = chain.request()
        val url = request.url.toString()
        if (!url.startsWith(SIMPLE_SCHEME)) return chain.proceed(request)

        val encoded = url.removePrefix(SIMPLE_SCHEME)
        val pageJson = String(Base64.decode(encoded, Base64.URL_SAFE or Base64.NO_WRAP), Charsets.UTF_8)
        val simplePage = json.decodeFromString<SimplePage>(pageJson)

        // إذا كان الرابط لا يزال صالحاً، استخدمه مباشرة
        if (!isTokenExpiringSoon(simplePage.originalUrl)) {
            val newRequest = request.newBuilder().url(simplePage.originalUrl).build()
            return chain.proceed(newRequest)
        }

        // جلب صفحة جديدة للحصول على رابط محدث
        val freshUnits = fetchFreshPageUnits(simplePage.chapterUrl, noCache = true)
        val freshUnit = freshUnits?.getOrNull(simplePage.pageIndex) as? RawPageUnit.Simple
        if (freshUnit == null) {
            // فشل التحديث: ارجع الصورة القديمة (قد تكون مكررة، لكن أفضل من فشل كامل)
            val fallbackRequest = request.newBuilder().url(simplePage.originalUrl).build()
            return chain.proceed(fallbackRequest)
        }

        val newRequest = request.newBuilder().url(freshUnit.imageUrl).build()
        return chain.proceed(newRequest)
    }

    // =====================================================================
    // Tiled Image Interceptor (للصفحات المقسّمة)
    // =====================================================================
    private fun tiledImageInterceptor(chain: Interceptor.Chain): Response {
        val request = chain.request()
        val url = request.url.toString()
        if (!url.startsWith(TILED_SCHEME)) {
            return chain.proceed(request)
        }

        val encoded = url.removePrefix(TILED_SCHEME)
        val pageJson = String(Base64.decode(encoded, Base64.URL_SAFE or Base64.NO_WRAP), Charsets.UTF_8)
        val tiledPage = json.decodeFromString<TiledPage>(pageJson)

        // تحديث التوكنات إذا لزم الأمر
        val refreshedPage = refreshTiledPageIfNeeded(tiledPage)
        if (refreshedPage.pieces.isEmpty()) {
            return Response.Builder()
                .request(request)
                .protocol(Protocol.HTTP_1_1)
                .code(500)
                .message("فشل تحديث الصورة المقسّمة")
                .body("".toResponseBody(null))
                .build()
        }

        val pieceHeaders = request.headers
        val mergedBytes = reconstructTiledPage(refreshedPage, pieceHeaders)
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

    private fun reconstructTiledPage(originalPage: TiledPage, pieceHeaders: Headers): ByteArray? {
        val page = refreshTiledPageIfNeeded(originalPage)
        if (page.pieces.isEmpty() || page.canvasWidth <= 0 || page.canvasHeight <= 0) return null

        val result = Bitmap.createBitmap(page.canvasWidth, page.canvasHeight, Bitmap.Config.ARGB_8888)
        val canvas = Canvas(result)
        var drewAny = false

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

    // =====================================================================
    // SCRAMBLED IMAGE INTERCEPTOR (قديم، نادر الاستخدام)
    // =====================================================================
    private fun scrambledImageInterceptor(chain: Interceptor.Chain): Response {
        val request = chain.request()
        val url = request.url.toString()
        if (!url.startsWith(SCRAMBLED_SCHEME)) {
            return chain.proceed(request)
        }

        val encoded = url.removePrefix(SCRAMBLED_SCHEME)
        val mapJson = String(Base64.decode(encoded, Base64.URL_SAFE or Base64.NO_WRAP), Charsets.UTF_8)
        val map = json.decodeFromString<ScrambledMap>(mapJson)

        val pieceHeaders = request.headers
        val mergedBytes = reconstructScrambledPage(map, pieceHeaders)
            ?: return Response.Builder()
                .request(request)
                .protocol(Protocol.HTTP_1_1)
                .code(500)
                .message("فشل دمج الصورة المشفرة")
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

    private fun reconstructScrambledPage(map: ScrambledMap, pieceHeaders: Headers): ByteArray? {
        val totalW = map.dim.getOrElse(0) { 800 }
        val totalH = map.dim.getOrElse(1) { 1200 }
        val n = map.pieces.size
        if (n == 0) return null

        val rawBitmaps = arrayOfNulls<Bitmap>(n)
        try {
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

    // =====================================================================
    // دوال مساعدة مشتركة
    // =====================================================================
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

    // =====================================================================
    // تحديث التوكنات (للمقسّمة والبسيطة)
    // =====================================================================
    private val expiresParamRegex = Regex("""[?&]expires=(\d+)""")

    // هامش أمان كبير (دقيقتان) لتجنب الانتهاء أثناء التحميل
    private fun isTokenExpiringSoon(url: String, safetyMarginSeconds: Long = 120L): Boolean {
        val expiresAt = expiresParamRegex.find(url)?.groupValues?.get(1)?.toLongOrNull() ?: return false
        val nowSeconds = System.currentTimeMillis() / 1000
        return expiresAt - nowSeconds < safetyMarginSeconds
    }

    private val freshPageUnitsCache = ConcurrentHashMap<String, Pair<Long, List<RawPageUnit>>>()
    private val freshUnitsCacheTtlMillis = 30_000L // 30 ثانية

    private fun fetchFreshPageUnits(chapterUrl: String, noCache: Boolean = false): List<RawPageUnit>? {
        val now = System.currentTimeMillis()
        if (!noCache) {
            freshPageUnitsCache[chapterUrl]?.let { (ts, units) ->
                if (now - ts < freshUnitsCacheTtlMillis) return units
            }
        }

        return try {
            // إضافة معلمة عشوائية لتجاوز الكاش
            val cacheBuster = "?_cb=${System.currentTimeMillis()}"
            val url = if (chapterUrl.contains("?")) "$chapterUrl$cacheBuster" else "$chapterUrl$cacheBuster"
            val resp = client.newCall(GET(url, headers)).execute()
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

    private fun refreshTiledPageIfNeeded(page: TiledPage): TiledPage {
        if (page.chapterUrl.isEmpty() || page.pageIndex < 0) return page
        val needsRefresh = page.pieces.any { isTokenExpiringSoon(it.url) }
        if (!needsRefresh) return page

        val freshUnits = fetchFreshPageUnits(page.chapterUrl, noCache = true) ?: return page
        val freshUnit = freshUnits.getOrNull(page.pageIndex) as? RawPageUnit.Tiled ?: return page

        return TiledPage(freshUnit.canvasWidth, freshUnit.canvasHeight, freshUnit.pieces, page.chapterUrl, page.pageIndex)
    }

    // =====================================================================
    // دوال التشفير القديمة (للصور المشفرة)
    // =====================================================================
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

    // =====================================================================
    // إحصائيات (views)
    // =====================================================================
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

    // =====================================================================
    // دوال غير مدعومة
    // =====================================================================
    override fun popularMangaRequest(page: Int): Request = throw UnsupportedOperationException()
    override fun popularMangaParse(response: Response): MangasPage = throw UnsupportedOperationException()
    override fun latestUpdatesRequest(page: Int): Request = throw UnsupportedOperationException()
    override fun latestUpdatesParse(response: Response): MangasPage = throw UnsupportedOperationException()
    override fun searchMangaParse(response: Response): MangasPage = throw UnsupportedOperationException()
    override fun imageUrlParse(response: Response): String = throw UnsupportedOperationException()
}

// =====================================================================
// تعريفات البيانات
// =====================================================================
private val SUPPORTED_TYPES = setOf("manga", "manhwa", "manhua", "webtoon", "comic")
private val JSON_MEDIA_TYPE = "application/json".toMediaType()
private val MOBILE_REGEX = Regex("mobile|android|iphone|ipad|ipod", RegexOption.IGNORE_CASE)
private val TABLES_REGEX = Regex("tablet", RegexOption.IGNORE_CASE)

@Serializable
data class TiledPiece(
    val url: String,
    val left: Double,
    val top: Double,
    val width: Double,
    val height: Double,
)

@Serializable
data class TiledPage(
    val canvasWidth: Int,
    val canvasHeight: Int,
    val pieces: List<TiledPiece>,
    val chapterUrl: String = "",
    val pageIndex: Int = -1,
)

@Serializable
data class SimplePage(
    val chapterUrl: String,
    val pageIndex: Int,
    val originalUrl: String,
)

// =====================================================================
// باقي تعريفات البيانات (مأخوذة من الكود الأصلي)
// =====================================================================
@Serializable
data class MetaData<T>(
    val data: List<T>,
    val meta: Meta,
)

@Serializable
data class Meta(
    val total: Int,
    val page: Int,
    val lastPage: Int,
    val hasNextPage: Boolean,
)

@Serializable
data class BrowseManga(
    val id: Int,
    val title: String,
    val slug: String,
    val type: String,
    val progress: String,
    val cdn: String? = null,
    val coverImage: String? = null,
    val coverImageApp: CoverImageApp? = null,
    val metadata: BrowseMetadata,
)

@Serializable
data class BrowseMetadata(
    val genres: List<String> = emptyList(),
    val tags: List<String> = emptyList(),
    val originalTitle: String? = null,
)

@Serializable
data class CoverImageApp(
    val desktop: String? = null,
    val mobile: String? = null,
)

@Serializable
data class Series(
    val series: SeriesDetail,
)

@Serializable
data class SeriesDetail(
    val id: Int,
    val title: String,
    val slug: String,
    val type: String,
    val progress: String?,
    val cdn: String? = null,
    val description: String? = null,
    val coverImageApp: CoverImageApp? = null,
    val metadata: SeriesMetadata,
)

@Serializable
data class SeriesMetadata(
    val author: List<String> = emptyList(),
    val artist: List<String> = emptyList(),
    val altTitles: List<String> = emptyList(),
    val originalTitle: String? = null,
    val year: String? = null,
    val origin: String? = null,
    val genres: List<String> = emptyList(),
    val tags: List<String> = emptyList(),
    val coverImage: String? = null,
)

@Serializable
data class InitialChapters(
    val initialChapters: List<Chapter>,
    val totalChapters: Int,
)

@Serializable
data class Chapter(
    val id: Int,
    val number: String,
    val title: String?,
    val language: String,
    val createdAt: String,
    val uploader: String? = null,
    val coins: Int? = null,
)

@Serializable
data class Data<T>(
    val data: T,
)

@Serializable
data class ChapterUrl(
    val id: Int,
    val number: String,
    val slug: String,
)

@Serializable
data class DeferredImages(
    val images: List<String>,
    val maps: List<ScrambledImage>,
)

@Serializable
data class ScrambledImage(
    val dim: List<Int> = emptyList(),
    val mode: String,
    val pieces: List<String>,
    val order: List<Int> = emptyList(),
)

@Serializable
data class ScrambledImageToken(
    val token: String,
)

@Serializable
data class ScrambledImageTokenValue(
    val iv: String,
    val tag: String,
    val data: String,
    val cid: Int,
    val m: String,
    val v: Int,
)

@Serializable
data class ScrambledMap(
    val dim: List<Int> = emptyList(),
    val mode: String,
    val pieces: List<String>,
    val order: List<Int> = emptyList(),
)

@Serializable
data class ChapterDeferredResponse(
    val success: Boolean,
    val data: DeferredImages? = null,
)

@Serializable
data class Key(
    val key: String,
)

@Serializable
data class ViewsDto(
    val chapterId: Int?,
    val contentId: Int,
    val deviceType: String,
    val surface: String,
)

// المرشحات
class TypeFilter : Filter.Select<String>(
    "النوع",
    arrayOf("الكل", "مانجا", "مانهوا", "مانها", "ويب تون"),
    arrayOf("", "manga", "manhua", "manhwa", "webtoon"),
) {
    val selected = if (state == 0) null else values[state]
}

class SortFilter : Filter.Select<String>(
    "الترتيب",
    arrayOf("آخر تحديث", "الأشهر", "الأعلى تقييماً", "الأكثر تعليقاً"),
    arrayOf("latest", "popular", "rating", "commented"),
) {
    val selected get() = values[state]
}

class YearFilter : Filter.Select<String>(
    "سنة الإصدار",
    arrayOf("الكل") + (2025 downTo 2010).map { it.toString() },
    arrayOf("") + (2025 downTo 2010).map { it.toString() },
) {
    val selected = if (state == 0) null else values[state]
}

class StatusFilter : Filter.Select<String>(
    "الحالة",
    arrayOf("الكل", "مستمر", "مكتمل", "متوقف"),
    arrayOf(null, "مستمر", "مكتمل", "متوقف"),
) {
    val selected get() = values[state]
}

open class MultiSelectFilter(name: String, val options: List<Pair<String, String>>) :
    Filter.Select<String>(name, options.map { it.first }.toTypedArray(), options.map { it.second }.toTypedArray()) {
    val selected get() = if (state == 0) emptyList() else listOf(values[state])
}

class GenreFilter : MultiSelectFilter(
    "التصنيف",
    genres
)

class TagFilter : MultiSelectFilter(
    "الوسم",
    tags
)

private val genres = listOf(
    "أكشن" to "Action",
    "مغامرة" to "Adventure",
    "كوميديا" to "Comedy",
    "دراما" to "Drama",
    "فنتازيا" to "Fantasy",
    "حريم" to "Harem",
    "رعب" to "Horror",
    "خيال علمي" to "Sci-fi",
    "رومانسية" to "Romance",
    "شونين" to "Shounen",
    "سينين" to "Seinen",
    "شوجو" to "Shoujo",
    "جوسي" to "Josei",
    "إيتشي" to "Ecchi",
    "فنون قتالية" to "Martial Arts",
    "تاريخي" to "Historical",
    "حياة مدرسية" to "School Life",
    "شريحة من الحياة" to "Slice of Life",
    "نفسي" to "Psychological",
    "غموض" to "Mystery",
    "مأساة" to "Tragedy",
    "خارق للطبيعة" to "Supernatural",
)

private val tags = listOf(
    "أكاديمية" to "Academy",
    "قدرات خارقة" to "Ability Steal",
    "مغامرون" to "Adventurers",
    "حبس" to "Confinement",
    "تطوير الشخصية" to "Character Growth",
    "ثنائي متشاجر" to "Bickering Couple",
    "ذكاء اصطناعي" to "Artificial Intelligence",
    "بطل بارد" to "Cold Protagonist",
    "بطل واثق" to "Confident Protagonist",
    "بطل جبان" to "Cowardly Protagonist",
    "بطل ماكر" to "Cunning Protagonist",
    "بطل حازم" to "Determined Protagonist",
    "بطل غير مبال" to "Apathetic Protagonist",
    "بطل اجتماعي" to "Anti-social Protagonist",
    "بطل غريب" to "Awkward Protagonist",
    "بطل كاريزمي" to "Charismatic Protagonist",
    "بطل طيب" to "Caring Protagonist",
    "بطل حذر" to "Cautious Protagonist",
    "بطل ساذج" to "Dense Protagonist",
    "بطل غير صادق" to "Dishonest Protagonist",
    "بطل مرتاب" to "Distrustful Protagonist",
    "تطور سريع" to "Accelerated Growth",
    "خطوبة مكسورة" to "Broken Engagement",
    "زواج مرتب" to "Arranged Marriage",
    "سكن مشترك" to "Cohabitation",
    "خيانة" to "Betrayal",
    "انتقام" to "Revenge",
    "اكتئاب" to "Depression",
    "حب الطفولة" to "Childhood Love",
    "صداقة الطفولة" to "Childhood Friends",
    "مشاكل عائلية" to "Complex Family Relationships",
    "حياة عائلية" to "Family Life",
    "قتلة مأجورون" to "Assassins",
    "جنود" to "Army",
    "سحر" to "Magic",
    "شياطين" to "Demons",
    "ملائكة" to "Angels",
    "وحوش" to "Beasts",
    "مخلوقات أسطورية" to "Mythical Creatures",
    "تحول" to "Transformation",
    "تناسخ" to "Reincarnation",
    "قدر" to "Destiny",
    "عالم موازي" to "Alternate World",
    "نهاية العالم" to "Apocalypse",
    "عالم خيالي" to "Fantasy World",
)
