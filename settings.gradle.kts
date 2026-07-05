import java.io.File

pluginManagement {
    repositories {
        val isCi = System.getenv().containsKey("CI") ||
                System.getenv().containsKey("GITHUB_ACTIONS") ||
                System.getenv().containsKey("JENKINS_HOME")

        if (!isCi) {
            maven(url = "https://mirrors.tencent.com/nexus/repository/maven-public/")
            maven(url = "https://maven.aliyun.com/repository/gradle-plugin")
        }

        gradlePluginPortal()
        mavenCentral()
        maven(url = "https://repo.spring.io/plugins-release")
    }

    plugins {
        id("org.openprojectx.spark.platform") version "0.1.42"
        id("org.openprojectx.bigdata-test") version "0.1.36"
    }
}

dependencyResolutionManagement {
    repositories {
        val isCi = System.getenv().containsKey("CI") ||
                System.getenv().containsKey("GITHUB_ACTIONS") ||
                System.getenv().containsKey("JENKINS_HOME")

        if (!isCi) {
            maven(url = "https://mirrors.tencent.com/nexus/repository/maven-public/")
            maven(url = "https://maven.aliyun.com/repository/public/")
        }

        mavenCentral()
    }
}

rootProject.name = "spark-lakehouse"

val excludeProjects: String? by settings

val buildFiles = fileTree(rootDir) {
    val excludes = excludeProjects?.split(",")
    include("**/*.gradle", "**/*.gradle.kts")
    exclude(
        "build",
        "**/gradle",
        "settings.gradle",
        "settings.gradle.kts",
        "buildSrc",
        "/build.gradle",
        "/build.gradle.kts",
        ".*",
        "out"
    )
    exclude("**/grails3")
    if (!excludes.isNullOrEmpty()) {
        exclude(excludes)
    }
}

val rootDirPath = rootDir.absolutePath + File.separator
buildFiles.forEach { buildFile ->
    val isDefaultName = buildFile.name.startsWith("build.gradle")
    val isKotlin = buildFile.name.endsWith(".kts")

    if (isDefaultName) {
        val buildFilePath = buildFile.parentFile.absolutePath
        val projectPath = buildFilePath
            .replace(rootDirPath, "")
            .replace(File.separator, ":")

        println("Adding project $projectPath")
        include(projectPath)
    } else {
        val projectName = if (isKotlin) {
            buildFile.name.removeSuffix(".gradle.kts")
        } else {
            buildFile.name.removeSuffix(".gradle")
        }

        val projectPath = ":$projectName"
        println("Adding project $projectPath")
        include(projectPath)

        val project = findProject(projectPath)
        project?.name = projectName
        project?.projectDir = buildFile.parentFile
        project?.buildFileName = buildFile.name
    }
}


gradle.extra["isCi"] = System.getenv().containsKey("CI") ||
        System.getenv().containsKey("GITHUB_ACTIONS") ||
        System.getenv().containsKey("JENKINS_HOME")
