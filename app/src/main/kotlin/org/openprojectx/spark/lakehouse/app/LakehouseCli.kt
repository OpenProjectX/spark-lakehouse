package org.openprojectx.spark.lakehouse.app

import com.typesafe.config.ConfigFactory
import java.io.File
import org.openprojectx.spark.lakehouse.core.JobConfigException
import org.openprojectx.spark.lakehouse.jobs.JobCatalog

private const val USAGE =
    "Usage: spark-lakehouse [--job <template>] [--profile <profiles>] --config <file>"

fun main(args: Array<String>) = LakehouseCli.run(args)

object LakehouseCli {

    fun run(args: Array<String>) {
        val parsed = parse(args)
        if (parsed.help) {
            println(USAGE)
            println("Templates: ${JobCatalog.names().sorted().joinToString(", ")}")
            return
        }
        if (parsed.profiles != null) {
            System.setProperty("spark.boot.profiles.active", parsed.profiles)
        }
        val configFile = File(parsed.configFile ?: error(USAGE))
        if (!configFile.isFile) {
            throw JobConfigException("Config file not found: ${configFile.absolutePath}")
        }
        val config = ConfigFactory.parseFile(configFile).resolve()

        val component = DaggerLakehouseComponent.create()
        try {
            LakehouseJobRunner.run(parsed.job, config, component)
        } finally {
            component.sparkSession().stop()
        }
    }

    internal data class CliArgs(
        val job: String?,
        val configFile: String?,
        val profiles: String?,
        val help: Boolean = false,
    )

    internal fun parse(args: Array<String>): CliArgs {
        var job: String? = null
        var configFile: String? = null
        var profiles: String? = null
        var help = false
        var index = 0
        while (index < args.size) {
            when (val arg = args[index]) {
                "--job" -> { job = args.getOrNull(++index) ?: error(USAGE) }
                "--config" -> { configFile = args.getOrNull(++index) ?: error(USAGE) }
                "--profile" -> { profiles = args.getOrNull(++index) ?: error(USAGE) }
                "--help", "-h" -> help = true
                else -> {
                    // bare argument = config file, mirroring spark-boot-cli
                    if (configFile == null && !arg.startsWith("--")) configFile = arg else error(USAGE)
                }
            }
            index++
        }
        return CliArgs(job, configFile, profiles, help)
    }
}
