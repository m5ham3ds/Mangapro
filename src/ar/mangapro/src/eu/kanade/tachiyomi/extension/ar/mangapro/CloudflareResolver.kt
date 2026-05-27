package eu.kanade.tachiyomi.extension.ar.mangapro

import android.annotation.SuppressLint
import android.app.Application
import android.os.Handler
import android.os.Looper
import android.view.View
import android.view.ViewGroup
import android.webkit.CookieManager
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import uy.kohesive.injekt.Injekt
import uy.kohesive.injekt.api.get
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Cloudflare Turnstile resolver with multi-strategy fallback.
 */
object CloudflareResolver {

    private const val TIMEOUT_SECONDS = 60L
    private const val POLL_INTERVAL_MS = 500L
    private const val WEBVIEW_WIDTH = 1080
    private const val WEBVIEW_HEIGHT = 1920
    private const val CLEARANCE_COOKIE = "cf_clearance"

    private val webViewTokenRegex = Regex(";\\s*wv\)")

    data class ResolveResult(
        val success: Boolean,
        val clearanceCookie: String? = null,
        val extraToken: String? = null,
        val allCookies: String? = null,
    )

    @Synchronized
    @SuppressLint("SetJavaScriptEnabled")
    fun resolve(
        loadUrl: String,
        cookieUrl: String = loadUrl,
        userAgent: String? = null,
        forceResolve: Boolean = false,
    ): Boolean {
        val result = resolveWithFullExtraction(loadUrl, cookieUrl, userAgent, forceResolve)
        return result.success
    }

    @Synchronized
    @SuppressLint("SetJavaScriptEnabled")
    fun resolveWithFullExtraction(
        loadUrl: String,
        cookieUrl: String = loadUrl,
        userAgent: String? = null,
        forceResolve: Boolean = false,
    ): ResolveResult {
        val cookieManager = CookieManager.getInstance()
        val existingCookies = cookieManager.getCookie(cookieUrl)

        if (!forceResolve) {
            val clearance = extractClearance(existingCookies)
            if (clearance != null) {
                return ResolveResult(
                    success = true,
                    clearanceCookie = clearance,
                    allCookies = existingCookies,
                )
            }
        }

        val context = Injekt.get<Application>()
        val handler = Handler(Looper.getMainLooper())
        val latch = CountDownLatch(1)
        var webView: WebView? = null
        var result: ResolveResult? = null
        lateinit var poll: Runnable

        handler.post {
            val wv = WebView(context)
            webView = wv

            wv.layoutParams = ViewGroup.LayoutParams(WEBVIEW_WIDTH, WEBVIEW_HEIGHT)
            wv.measure(
                View.MeasureSpec.makeMeasureSpec(WEBVIEW_WIDTH, View.MeasureSpec.EXACTLY),
                View.MeasureSpec.makeMeasureSpec(WEBVIEW_HEIGHT, View.MeasureSpec.EXACTLY),
            )
            wv.layout(0, 0, WEBVIEW_WIDTH, WEBVIEW_HEIGHT)

            with(wv.settings) {
                javaScriptEnabled = true
                domStorageEnabled = true
                databaseEnabled = true
                loadWithOverviewMode = true
                useWideViewPort = true
                blockNetworkImage = false
                mediaPlaybackRequiresUserGesture = false
                setSupportMultipleWindows(false)
                javaScriptCanOpenWindowsAutomatically = false
                loadsImagesAutomatically = true
                val baseUa = userAgent ?: WebSettings.getDefaultUserAgent(context)
                userAgentString = baseUa.replace(webViewTokenRegex, ")")
            }

            cookieManager.setAcceptCookie(true)
            cookieManager.setAcceptThirdPartyCookies(wv, true)

            wv.webViewClient = object : WebViewClient() {
                override fun onPageFinished(view: WebView?, url: String?) {
                    super.onPageFinished(view, url)
                    extractAllTokens(view) { tokens ->
                        if (tokens.isNotEmpty()) {
                            val clearance = tokens.find { it.startsWith("$CLEARANCE_COOKIE=") }
                                ?.substringAfter("=", "")
                            val extra = tokens.find { !it.startsWith("$CLEARANCE_COOKIE=") }
                                ?.substringAfter("=", "")
                            result = ResolveResult(
                                success = clearance != null || extra != null,
                                clearanceCookie = clearance,
                                extraToken = extra,
                                allCookies = tokens.joinToString("; "),
                            )
                            if (result?.success == true) {
                                latch.countDown()
                            }
                        }
                    }
                }
            }

            poll = Runnable {
                if (latch.count == 0L) return@Runnable
                val currentCookies = cookieManager.getCookie(cookieUrl)
                val clearance = extractClearance(currentCookies)
                if (clearance != null) {
                    result = ResolveResult(
                        success = true,
                        clearanceCookie = clearance,
                        allCookies = currentCookies,
                    )
                    latch.countDown()
                } else {
                    handler.postDelayed(poll, POLL_INTERVAL_MS)
                }
            }

            wv.loadUrl(loadUrl)
            handler.postDelayed(poll, POLL_INTERVAL_MS)
        }

        latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)

        handler.post {
            handler.removeCallbacks(poll)
            webView?.stopLoading()
            webView?.destroy()
        }

        return result ?: ResolveResult(success = false)
    }

    private fun extractAllTokens(view: WebView?, callback: (List<String>) -> Unit) {
        val js = buildString {
            append("(function() {")
            append("var results = [];")
            append("try { results.push('cookies=' + document.cookie); } catch(e) {}")
            append("var tokenKeys = ['actionToken','__cf_token','_cf_chl_opt','turnstileToken','cf_token','__turnstileToken','__cf_chl_token','cf_chl_seq','__cf_chl_ctx','_cf_chl_enter','_cf_chl_done'];")
            append("for (var i = 0; i < tokenKeys.length; i++) {")
            append("try {")
            append("var val = window[tokenKeys[i]];")
            append("if (val && typeof val === 'string' && val.length > 8) { results.push(tokenKeys[i] + '=' + val); }")
            append("else if (val && typeof val === 'object' && val.token) { results.push(tokenKeys[i] + '=' + String(val.token)); }")
            append("} catch(e) {}")
            append("}")
            append("try {")
            append("var inputs = document.querySelectorAll('input[type=\"hidden\"]');")
            append("for (var j = 0; j < inputs.length; j++) {")
            append("var name = inputs[j].name || inputs[j].id || '';")
            append("var val = inputs[j].value;")
            append("if (val && val.length > 10 && (name.indexOf('token') >= 0 || name.indexOf('cf') >= 0)) { results.push('input_' + name + '=' + val); }")
            append("}")
            append("} catch(e) {}")
            append("try {")
            append("var turnstile = document.querySelector('[data-turnstile-sitekey], .cf-turnstile, #turnstile');")
            append("if (turnstile) { results.push('turnstile_present=true'); }")
            append("} catch(e) {}")
            append("return results.join('|||');")
            append("})()")
        }

        view?.evaluateJavascript(js) { value ->
            val raw = value?.trim('"')?.replace("\"", """)?.takeIf { it != "null" && it != "undefined" }
            if (raw.isNullOrEmpty()) {
                callback(emptyList())
                return@evaluateJavascript
            }

            val tokens = raw.split("|||").filter { it.isNotBlank() }
            val allCookies = mutableListOf<String>()
            tokens.forEach { token ->
                when {
                    token.startsWith("cookies=") -> {
                        val cookieStr = token.substringAfter("cookies=")
                        if (cookieStr.isNotBlank()) {
                            cookieStr.split(";").forEach { c ->
                                val trimmed = c.trim()
                                if (trimmed.isNotBlank()) allCookies.add(trimmed)
                            }
                        }
                    }
                    token.contains("=") -> allCookies.add(token)
                }
            }
            callback(allCookies)
        }
    }

    private fun extractClearance(cookies: String?): String? {
        if (cookies.isNullOrBlank()) return null
        return cookies.split(";").map { it.trim() }
            .firstOrNull { it.startsWith("$CLEARANCE_COOKIE=") }
            ?.substringAfter("=", "")
            ?.takeIf { it.isNotBlank() }
    }
}
