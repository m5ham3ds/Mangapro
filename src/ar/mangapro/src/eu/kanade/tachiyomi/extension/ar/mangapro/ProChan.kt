package eu.kanade.tachiyomi.extension.ar.mangapro

import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.Canvas
import android.graphics.Rect
import android.util.Base64
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
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.Callback
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.Interceptor
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Protocol
import okhttp3.Request
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
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

class ProChan : HttpSource() {
    override val name = "ProChan"
    override val lang = "ar"
    private val domain = "procomic.net"
    override val baseUrl = "https://$domain"
    override val supportsLatest = true
    override val versionId = 10

    private val json = Json { ignoreUnknownKeys = true }

    companion object {
        private const val SCRAMBLED_SCHEME = "https://procomic.net/__scrambled__/?map="
        private const val MAX_DEFERRED_SPLITS = 20
        private const val EMPTY_THRESHOLD = 2
    }

    override val client = network.cloudflareClient.newBuilder()
        .addInterceptor(::scrambledImageInterceptor)
        .addNetworkInterceptor(
            CookieInterceptor(
                domain,
                listOf(
                    "safe_browsing" to "off",
                    "language" to "ar",
                ),
            ),
        )
        .build()

    override fun headersBuilder() = super.headersBuilder()
        .set("Referer", "$baseUrl/")
        .set("Origin", baseUrl)
        .set("Accept-Language", "ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7")

    private val rscHeaders = headersBuilder()
        .set("rsc", "1")
        .build()

    // =================================================================
    // POPULAR / LATEST / SEARCH
    // =================================================================
    override fun fetchPopularManga(page: Int): Observable<MangasPage> {
        val filters = getFilterList().apply {
            firstInstance<<SortFilter>().state = 2
        }
        return fetchSearchManga(page, "", filters)
    }

    override fun fetchLatestUpdates(page: Int): Observable<MangasPage> {
        val filters = getFilterList().apply {
            firstInstance<<SortFilter>().state = 1
        }
        return fetchSearchManga(page, "", filters)
    }

    override fun fetchSearchManga(page: Int, query: String, filters: FilterList): Observable<MangasPage> {
        if (query.startsWith("https://")) {
            val url = query.toHttpUrl()
            val path = url.pathSegments
            if (url.host == domain && path.size >= 4 && path[0] == "series") {
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
                        throw Exception("تم حظر الوصول بواسطة Cloudflare. جرب فتح الموقع في WebView أولاً.")
                    }
                    throw Exception("HTTP ${response.code}")
                }

                val statusFilter = filters.firstInstance<<StatusFilter>().selected
                val genreFilter = filters.firstInstance<<GenreFilter>()
                val tagFilter = filters.firstInstance<TagFilter>()

                val data = response.parseAs<<MetaData<List<BrowseManga>>>()
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
                                if (it.startsWith("/")) manga.cdn?.let { cdn -> "https://$cdn.$domain$it" } else it
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
            addQueryParameter("sort", filters.firstInstance<<SortFilter>().selected)
            filters.firstInstance<<YearFilter>().selected?.also { addQueryParameter("year", it) }
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
            if (response.code == 403) throw Exception("تم حظر الوصول بواسطة Cloudflare")
            throw Exception("HTTP ${response.code}")
        }

        val manga = response.extractNextJs<<SeriesWrapper>()!!.series
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
                    val genreMap = genresList.associate { it.second to it.first }
                    manga.metadata.genres.mapTo(this) { genreMap[it] ?: it }
                }
                if (manga.metadata.tags.isNotEmpty()) {
                    val tagsMap = tagsList.associate { it.second to it.first }
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
                if (it.startsWith("/")) manga.cdn?.let { cdn -> "https://$cdn.$domain$it" } else it
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
            if (response.code == 403) throw Exception("تم حظر الوصول بواسطة Cloudflare")
            throw Exception("HTTP ${response.code}")
        }

        val data = response.extractNextJs<<InitialChapters>()!!
        val chapters = data.initialChapters.toMutableList()
        val size = chapters.size
        var page = 2
        val type = response.request.url.pathSegments[1]
        val id = response.request.url.pathSegments[2]
        val slug = response.request.url.pathSegments[3]

        while (data.totalChapters > chapters.size) {
            val request = GET("$baseUrl/api/public/$type/$id/chapters?page=${page++}&limit=$size&order=desc", headers)
            val nextResponse = client.newCall(request).execute()
            if (!nextResponse.isSuccessful) {
                nextResponse.close()
                if (nextResponse.code == 403) {
                    throw Exception("تم حظر الوصول بواسطة Cloudflare عند جلب الصفحة ${page - 1}")
                }
                throw Exception("HTTP ${nextResponse.code} - فشل جلب الصفحة ${page - 1}")
            }
            val nextChapters = nextResponse.parseAs<Data<List<<Chapter>>>()
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
    // PAGE LIST — Bulletproof Implementation
    // =================================================================
    override fun pageListRequest(chapter: SChapter): Request = GET(getChapterUrl(chapter), rscHeaders)

    override fun getChapterUrl(chapter: SChapter): String {
        val url = if (chapter.url.startsWith("{")) chapter.url.parseAs<<ChapterUrl>().url else chapter.url
        return "$baseUrl$url"
    }

    override fun fetchPageList(chapter: SChapter): Observable<List<<Page>> {
        return Observable.fromCallable {
            val request = pageListRequest(chapter)
            val response = client.newCall(request).execute()
            if (!response.isSuccessful) {
                response.close()
                if (response.code == 403) {
                    throw Exception("تم حظر الوصول بواسطة Cloudflare. جرب فتح الموقع في WebView أولاً.")
                }
                throw Exception("HTTP ${response.code} - فشل جلب صفحات الفصل")
            }
            pageListParse(response)
        }
    }

    override fun pageListParse(response: Response): List<<Page> {
        val html = response.body.string()

        // ─── استخراج كل شيء ممكن من HTML ───
        val embeddedImages = extractAllImageUrls(html)
        val embeddedMaps = extractAllMaps(html)
        val deferredToken = extractAnyDeferredToken(html)

        val pages = mutableListOf<<Page>()
        val existingUrls = mutableSetOf<String>()
        var index = 0

        // إضافة الصور المضمّنة
        embeddedImages.forEach { url ->
            pages.add(Page(index++, imageUrl = url))
            existingUrls.add(url)
        }

        // إضافة Maps المضمّنة
        embeddedMaps.forEach { map ->
            if (map.pieces.isEmpty()) return@forEach
            val encoded = encodeMap(map)
            pages.add(Page(index++, imageUrl = encoded))
            existingUrls.add(map.pieces.first())
        }

        // إذا لم يوجد Token → نعيد ما لدينا (قد يكون كاملاً للفصول القصيرة)
        if (deferredToken == null) {
            if (pages.isEmpty()) {
                throw Exception("لم يتم العثور على صفحات. الموقع قد يكون قد تغيّر أو هناك مشكلة في الاستخراج.")
            }
            return pages
        }

        val chapterId = response.request.url.pathSegments.getOrNull(4)
            ?: throw Exception("معرف الفصل غير موجود في الرابط")
        val seriesId = response.request.url.pathSegments.getOrNull(2)
            ?: throw Exception("معرف السلسلة غير موجود في الرابط")

        val apiHeaders = headers.newBuilder()
            .set("Accept", "application/json")
            .set("Referer", response.request.url.toString())
            .build()

        // ─── جلب جميع الصفحات المؤجلة بذكاء ───
        val deferredResult = fetchAllDeferredPages(chapterId, deferredToken, apiHeaders)

        // إضافة الصور المؤجلة مع deduplication
        deferredResult.images.forEach { url ->
            if (existingUrls.add(url)) {
                pages.add(Page(index++, imageUrl = url))
            }
        }

        // إضافة Maps المؤجلة مع deduplication
        deferredResult.maps.forEach { map ->
            if (map.pieces.isEmpty()) return@forEach
            val key = map.pieces.first()
            if (existingUrls.add(key)) {
                pages.add(Page(index++, imageUrl = encodeMap(map)))
            }
        }

        if (pages.isEmpty()) {
            throw Exception("فشل استخراج جميع الصفحات. جرب فتح الفصل في WebView.")
        }

        countViews(seriesId, chapterId)
        return pages
    }

    // ═══════════════════════════════════════════════════════════════
    // DEFERRED FETCHING — Auto-discovery with empty-split tolerance
    // ═══════════════════════════════════════════════════════════════
    private fun fetchAllDeferredPages(
        chapterId: String,
        token: String,
        apiHeaders: okhttp3.Headers,
    ): DeferredResult {
        val allImages = mutableListOf<String>()
        val allMaps = mutableListOf<<ScrambledMap>()

        // جلب split=0 مع إعادة محاولة واحدة
        val firstResult = retry(times = 2, delayMs = 800) {
            executeDeferredRequest(chapterId, token, 0, apiHeaders)
        }

        if (firstResult == null || !firstResult.success || firstResult.data == null) {
            // نعيد فارغًا — سنعتمد على الصفحات المضمّنة فقط
            return DeferredResult(allImages, allMaps)
        }

        allImages.addAll(firstResult.data.images)
        allMaps.addAll(firstResult.data.maps)

        val hintIndex = firstResult.data.splitIndex
        val maxSplit = maxOf(hintIndex, MAX_DEFERRED_SPLITS)

        // جلب splits حتى نحصل على توقفين فارغين/فاشلين متتاليين
        var consecutiveEmpty = 0
        for (s in 1..maxSplit) {
            val result = runCatching {
                executeDeferredRequest(chapterId, token, s, apiHeaders)
            }.getOrNull()

            val hasContent = result?.success == true &&
                result.data != null &&
                (result.data.images.isNotEmpty() || result.data.maps.isNotEmpty())

            if (hasContent) {
                allImages.addAll(result.data!!.images)
                allMaps.addAll(result.data.maps)
                consecutiveEmpty = 0
            } else {
                consecutiveEmpty++
                if (consecutiveEmpty >= EMPTY_THRESHOLD) break
            }

            // إذا تجاوزنا hintIndex ولم نجد محتوى، نتوقف مبكرًا
            if (s > hintIndex && consecutiveEmpty > 0) break
        }

        return DeferredResult(allImages, allMaps)
    }

    private fun executeDeferredRequest(
        chapterId: String,
        token: String,
        split: Int,
        apiHeaders: okhttp3.Headers,
    ): ChapterDeferredResponse? {
        val request = GET(
            "$baseUrl/chapter-deferred-media/$chapterId?token=$token&split=$split",
            apiHeaders,
        )
        return client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                if (response.code == 403) {
                    throw Exception("Cloudflare حظر جلب الصفحة المؤجلة")
                }
                return@use null
            }
            response.parseAs<<ChapterDeferredResponse>()
        }
    }

    private inline fun <T> retry(times: Int, delayMs: Long = 500, block: () -> T): T? {
        var lastError: Exception? = null
        repeat(times) { attempt ->
            try {
                return block()
            } catch (e: Exception) {
                lastError = e
                if (attempt < times - 1) {
                    try {
                        Thread.sleep(delayMs)
                    } catch (_: InterruptedException) {}
                }
            }
        }
        return null
    }

    // ═══════════════════════════════════════════════════════════════
    // EXTRACTION — Multiple cascading methods, no logs needed
    // ═══════════════════════════════════════════════════════════════

    /** استخراج جميع روابط الصور بـ 3 طرق متتالية */
    private fun extractAllImageUrls(html: String): List<String> {
        val results = mutableListOf<String>()

        // الطريقة 1: JSON Array Parser (الأدق)
        try {
            extractJsonArray(html, "images")?.let { arrayJson ->
                val urls = json.decodeFromString<List<String>>(arrayJson)
                if (urls.isNotEmpty()) return urls.distinct()
            }
        } catch (_: Exception) {}

        // الطريقة 2: Regex مرن + استخراج يدوي
        try {
            val relaxed = Regex("""\"images\"\s*:\s*\[(.*?)\]""", RegexOption.DOT_MATCHES_ALL)
            relaxed.findAll(html).forEach { match ->
                val content = match.groupValues[1]
                Regex("""\"(https?://[^\"]+)\"""").findAll(content).forEach { urlMatch ->
                    results.add(urlMatch.groupValues[1])
                }
            }
            if (results.isNotEmpty()) return results.distinct()
        } catch (_: Exception) {}

        // الطريقة 3: بحث شامل في HTML (احتياطي أخير)
        try {
            Regex("""(https?://[^\s\"<>]+?\.(?:avif|jpg|jpeg|png|webp|gif))""", RegexOption.IGNORE_CASE)
                .findAll(html)
                .map { it.groupValues[1] }
                .filter { it.contains("procomic") || it.contains("cdn") }
                .forEach { results.add(it) }
        } catch (_: Exception) {}

        return results.distinct()
    }

    /** استخراج جميع Maps بـ طريقتين */
    private fun extractAllMaps(html: String): List<<ScrambledMap> {
        // الطريقة 1: JSON Array
        try {
            extractJsonArray(html, "maps")?.let { arrayJson ->
                return json.decodeFromString<List<<ScrambledMap>>(arrayJson)
            }
        } catch (_: Exception) {}

        // الطريقة 2: Regex مرن
        try {
            val relaxed = Regex("""\"maps\"\s*:\s*(\[.*?\])""", RegexOption.DOT_MATCHES_ALL)
            relaxed.find(html)?.groupValues?.get(1)?.let { mapsJson ->
                return json.decodeFromString<List<<ScrambledMap>>(mapsJson)
            }
        } catch (_: Exception) {}

        return emptyList()
    }

    /** استخراج الـ Token بـ 3 طرق متتالية */
    private fun extractAnyDeferredToken(html: String): String? {
        // 1: JSON Object Parser
        try {
            extractJsonObject(html, "deferredMedia")?.let { objJson ->
                val obj = json.decodeFromString<JsonObject>(objJson)
                obj["token"]?.jsonPrimitive?.content?.takeIf { it.isNotBlank() }?.let { return it }
            }
        } catch (_: Exception) {}

        // 2: Regex مباشر مع احترام المسافات
        Regex("""\"deferredMedia\"\s*:\s*\{\s*\"token\"\s*:\s*\"([^\"]+)\"""")
            .find(html)?.groupValues?.get(1)?.takeIf { it.isNotBlank() }?.let { return it }

        // 3: JWT في أي مكان
        Regex("""eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+""")
            .find(html)?.value?.let { return it }

        return null
    }

    // ─── JSON Extraction Helpers (Bracket-aware) ───
    private fun extractJsonArray(html: String, key: String): String? {
        val prefix = "\"$key\":"
        val idx = html.indexOf(prefix)
        if (idx == -1) return null

        val start = html.indexOf('[', idx + prefix.length)
        if (start == -1) return null

        var depth = 0
        var inString = false
        var escape = false

        for (i in start until html.length) {
            val c = html[i]
            if (escape) {
                escape = false
                continue
            }
            if (c == '\\') {
                escape = true
                continue
            }
            if (c == '"' && !inString) {
                inString = true
                continue
            }
            if (c == '"' && inString) {
                inString = false
                continue
            }
            if (!inString) {
                if (c == '[') depth++
                if (c == ']') {
                    depth--
                    if (depth == 0) {
                        return html.substring(start, i + 1)
                    }
                }
            }
        }
        return null
    }

    private fun extractJsonObject(html: String, key: String): String? {
        val prefix = "\"$key\":"
        val idx = html.indexOf(prefix)
        if (idx == -1) return null

        val start = html.indexOf('{', idx + prefix.length)
        if (start == -1) return null

        var depth = 0
        var inString = false
        var escape = false

        for (i in start until html.length) {
            val c = html[i]
            if (escape) {
                escape = false
                continue
            }
            if (c == '\\') {
                escape = true
                continue
            }
            if (c == '"' && !inString) {
                inString = true
                continue
            }
            if (c == '"' && inString) {
                inString = false
                continue
            }
            if (!inString) {
                if (c == '{') depth++
                if (c == '}') {
                    depth--
                    if (depth == 0) {
                        return html.substring(start, i + 1)
                    }
                }
            }
        }
        return null
    }

    private fun encodeMap(map: ScrambledMap): String {
        val encoded = Base64.encodeToString(
            json.encodeToString(ScrambledMap.serializer(), map).toByteArray(Charsets.UTF_8),
            Base64.URL_SAFE or Base64.NO_WRAP,
        )
        return "$SCRAMBLED_SCHEME$encoded"
    }

    override fun imageRequest(page: Page): Request {
        val headers = headersBuilder()
            .set("Referer", page.url)
            .build()
        return GET(page.imageUrl!!, headers)
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
        val map = json.decodeFromString<<ScrambledMap>(mapJson)

        val mergedBytes = reconstructPage(map)
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

    // ════════════════════════════════════════════════════════
    // IMAGE RECONSTRUCTION
    // ════════════════════════════════════════════════════════
    private fun reconstructPage(map: ScrambledMap): ByteArray? {
        val totalW = map.dim.getOrElse(0) { 800 }
        val totalH = map.dim.getOrElse(1) { 1200 }
        val n = map.pieces.size
        if (n == 0) return null

        val rawBitmaps = arrayOfNulls<<Bitmap>(n)
        try {
            for (i in 0 until n) {
                try {
                    val resp = client.newCall(
                        Request.Builder()
                            .url(map.pieces[i])
                            .header("Referer", "$baseUrl/")
                            .header("Accept", "image/avif,image/webp,image/jpeg,*/*")
                            .header("User-Agent", headers["User-Agent"] ?: "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36")
                            .build(),
                    ).execute()
                    val bytes = resp.body.bytes()
                    resp.close()
                    rawBitmaps[i] = decodeAvif(bytes)
                } catch (_: Exception) {}
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
        } catch (_: Exception) {
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
                } catch (_: Exception) {
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

    // =================================================================
    // TOKEN DECRYPTION (Legacy scrambled images)
    // =================================================================
    private val sessionKey = ConcurrentHashMap<Int, Pair<String, Long>>()
    private val sessionKeyLock = Any()

    private fun decodeScrambledImageToken(data: ScrambledImageToken): ScrambledImage {
        val value = String(urlSafeBase64(data.token), Charsets.UTF_8)
            .parseAs<<ScrambledImageTokenValue>()

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
                            throw Exception("تم حظر الوصول بواسطة Cloudflare عند جلب مفتاح الجلسة")
                        }
                        throw Exception("HTTP $code - فشل جلب مفتاح الصورة المشفرة")
                    }
                    val keyData = response.parseAs<Data<KeyData>>()
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

    // =================================================================
    // VIEWS COUNTING
    // =================================================================
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
                    override fun onResponse(call: okhttp3.Call, response: Response) {
                        response.closeQuietly()
                    }
                    override fun onFailure(call: okhttp3.Call, e: IOException) {}
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

// =================================================================
// CONSTANTS
// =================================================================
private val SUPPORTED_TYPES = setOf("manga", "manhwa", "manhua", "webtoon", "comic")
private val JSON_MEDIA_TYPE = "application/json".toMediaType()
private val MOBILE_REGEX = Regex("mobile|android|iphone|ipad|ipod", RegexOption.IGNORE_CASE)
private val TABLES_REGEX = Regex("tablet", RegexOption.IGNORE_CASE)

// Genre & Tag lists (used by mangaDetailsParse)
private val genresList = listOf(
    "أكشن" to "action",
    "مغامرة" to "adventure",
    "كوميديا" to "comedy",
    "دراما" to "drama",
    "فنتازيا" to "fantasy",
    "رعب" to "horror",
    "غموض" to "mystery",
    "رومانسية" to "romance",
    "خيال علمي" to "sci-fi",
    " Slice of Life" to "slice-of-life",
    "رياضة" to "sports",
    "خارق للطبيعة" to "supernatural",
    "إثارة" to "thriller",
    "حرب" to "war",
    "نفسي" to "psychological",
    "مدرسي" to "school",
    "سينين" to "seinen",
    "شونين" to "shounen",
    "شوجو" to "shoujo",
    "جوسي" to "josei",
    "تاريخي" to "historical",
    "سحر" to "magic",
    "فنون قتالية" to "martial-arts",
    "موسيقى" to "music",
    "آلي" to "mecha",
    "شرطة" to "police",
    "قوى خارقة" to "super-powers",
    "فضاء" to "space",
    "نوادٍ" to "clubs",
    "ألعاب" to "games",
    "بارودي" to "parody",
    "شرير" to "villain",
    "انتقام" to "revenge",
    "بقاء" to "survival",
    "حياة يومية" to "daily-life",
    "عائلي" to "family",
    "حيوانات" to "animals",
    "طبخ" to "cooking",
    "طب" to "medical",
)

private val tagsList = listOf(
    "قوى خارقة" to "super-powers",
    "سحر" to "magic",
    "فنون قتالية" to "martial-arts",
    "انتقام" to "revenge",
    "بقاء" to "survival",
    "شرير" to "villain",
    "حياة يومية" to "daily-life",
    "عائلي" to "family",
    "حيوانات" to "animals",
    "طبخ" to "cooking",
    "طب" to "medical",
    "تاريخي" to "historical",
    "موسيقى" to "music",
    "آلي" to "mecha",
    "شرطة" to "police",
    "فضاء" to "space",
    "نوادٍ" to "clubs",
    "ألعاب" to "games",
    "بارودي" to "parody",
    "سينين" to "seinen",
    "شونين" to "shounen",
    "شوجو" to "shoujo",
    "جوسي" to "josei",
    "مدرسي" to "school",
    "رياضة" to "sports",
    "نفسي" to "psychological",
    "إثارة" to "thriller",
    "حرب" to "war",
    "خارق للطبيعة" to "supernatural",
    "رومانسية" to "romance",
    "غموض" to "mystery",
    "رعب" to "horror",
    "فنتازيا" to "fantasy",
    "دراما" to "drama",
    "كوميديا" to "comedy",
    "مغامرة" to "adventure",
    "أكشن" to "action",
)

// =================================================================
// DTOs
// =================================================================

@Serializable
private data class MetaData<T>(val data: T, val meta: Meta)

@Serializable
private data class Meta(val total: Int, val page: Int, val limit: Int) {
    fun hasNextPage() = total > page * limit
}

@Serializable
private data class BrowseManga(
    val id: String,
    val type: String,
    val slug: String,
    val title: String,
    val coverImage: String?,
    val coverImageApp: CoverImageApp?,
    val cdn: String?,
    val progress: String?,
    val metadata: MangaMetadata,
)

@Serializable
private data class CoverImageApp(val desktop: String?)

@Serializable
private data class MangaMetadata(
    val artist: List<String> = emptyList(),
    val author: List<String> = emptyList(),
    val altTitles: List<String> = emptyList(),
    val originalTitle: String? = null,
    val year: String? = null,
    val origin: String? = null,
    val genres: List<String> = emptyList(),
    val tags: List<String> = emptyList(),
    val coverImage: String? = null,
)

@Serializable
private data class SeriesWrapper(val series: SeriesData)

@Serializable
private data class SeriesData(
    val id: String,
    val type: String,
    val slug: String,
    val title: String,
    val description: String?,
    val coverImageApp: CoverImageApp?,
    val cdn: String?,
    val progress: String?,
    val metadata: MangaMetadata,
)

@Serializable
private data class InitialChapters(
    val initialChapters: List<<Chapter>,
    val totalChapters: Int,
)

@Serializable
private data class Chapter(
    val id: String,
    val number: String,
    val title: String?,
    val coins: Int? = null,
    val language: String,
    val uploader: String?,
    val createdAt: String,
)

@Serializable
private data class ChapterUrl(val url: String)

@Serializable
private data class Data<T>(val data: T)

@Serializable
private data class KeyData(val key: String)

@Serializable
private data class ChapterDeferredResponse(
    val success: Boolean,
    val data: ChapterDeferredData?,
)

@Serializable
private data class ChapterDeferredData(
    val splitIndex: Int,
    val images: List<String> = emptyList(),
    val maps: List<<ScrambledMap> = emptyList(),
)

@Serializable
private data class ScrambledMap(
    val dim: List<Int>,
    val mode: String,
    val order: List<Int>,
    val pieces: List<String>,
)

@Serializable
private data class ScrambledImageToken(val token: String)

@Serializable
private data class ScrambledImageTokenValue(
    val iv: String,
    val tag: String,
    val data: String,
    val m: String,
    val v: Int,
    val cid: Int,
)

@Serializable
private data class ScrambledImage(val url: String)

@Serializable
private data class ViewsDto(
    val chapterId: Int?,
    val contentId: Int,
    val deviceType: String,
    val surface: String,
)

// Helper for deferred fetching
private data class DeferredResult(
    val images: List<String>,
    val maps: List<<ScrambledMap>,
)
