package org.openprojectx.spark.lakehouse.core

import com.typesafe.config.Config

/**
 * Fail-fast accessors over a submitted HOCON job config. Every failure names
 * the exact path so tenant configs can be fixed without reading template code.
 */
object ConfigSupport {

    fun requiredString(config: Config, path: String): String {
        if (!config.hasPath(path)) {
            throw JobConfigException("Missing required config '$path'")
        }
        val value = config.getString(path)
        if (value.isBlank()) {
            throw JobConfigException("Config '$path' must not be blank")
        }
        return value
    }

    fun optionalString(config: Config, path: String): String? =
        if (config.hasPath(path)) config.getString(path).ifBlank { null } else null

    fun optionalInt(config: Config, path: String): Int? =
        if (config.hasPath(path)) config.getInt(path) else null

    fun stringList(config: Config, path: String): List<String> =
        if (config.hasPath(path)) config.getStringList(path) else emptyList()

    fun stringMap(config: Config, path: String): Map<String, String> =
        if (config.hasPath(path)) {
            config.getConfig(path).entrySet().associate { it.key to it.value.unwrapped().toString() }
        } else {
            emptyMap()
        }
}
