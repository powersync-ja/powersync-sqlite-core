import org.gradle.tooling.BuildException
import java.util.Base64
import java.util.Properties
import kotlin.io.path.Path
import kotlin.io.path.absolutePathString
import kotlin.io.path.exists
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name

plugins {
    id("maven-publish")
    id("signing")
}

group = "co.powersync"
version = "0.4.1"
description = "PowerSync Core SQLite Extension"

val localRepo = uri("build/repository/")

repositories {
    mavenCentral()
    google()
}

fun ndkPath(): String {
    val file = project.rootProject.file("local.properties")
    var androidHome = System.getenv("ANDROID_HOME")

    if (file.exists()) {
        val properties = Properties()
        properties.load(project.rootProject.file("local.properties").inputStream())

        properties["sdk.dir"]?.let {
            androidHome = it as String
        }
    }

    check(androidHome != null) { "Could not find android SDK dir" }

    val ndks = Path(androidHome).resolve("ndk")
    check(ndks.exists()) { "Expected NDK installations at $ndks" }

    for (entry in ndks.listDirectoryEntries()) {
        val name = entry.name
        val majorVersion = name.split('.').first().toInt()

        // We want to use NDK 28 or newer to build with 16KB support by default.
        if (majorVersion >= 28) {
            return entry.absolutePathString()
        }
    }

    error("Expected an NDK 28 or later installation in $ndks")
}

val buildRust = tasks.register<Exec>("buildRust") {
    group = "build"
    environment("ANDROID_NDK_HOME", ndkPath())

    workingDir("..")
    commandLine(
        "cargo",
        "ndk",
        "-t",
        "armeabi-v7a",
        "-t",
        "arm64-v8a",
        "-t",
        "x86",
        "-t",
        "x86_64",
        "-o",
        "./android/build/intermediates/jniLibs",
        "build",
        "--release",
        "-Zbuild-std",
        "-p",
        "powersync_loadable"
    )
}

val prefabAar = tasks.register<Zip>("prefabAar") {
    dependsOn(buildRust)

    from("build/intermediates/jniLibs") {
        include("**/*")
        into("jni")
    }

    from("src/") {
        include("**/*")
    }

    val architectures = listOf(
        "armeabi-v7a",
        "arm64-v8a",
        "x86",
        "x86_64"
    )

    architectures.forEach { architecture ->
        from("build/intermediates/jniLibs/$architecture/") {
            include("libpowersync.so")
            into("prefab/modules/powersync/libs/android.$architecture/")
        }
    }

    archiveFileName.set("build/outputs/aar/powersync-sqlite-core.aar")
    destinationDirectory.set(file("./"))
}

val sourcesJar = tasks.register<Jar>("sourcesJar") {
    // We don't have any actual java sources to bundle
    archiveClassifier.set("sources")
}

publishing {
    publications {
        register<MavenPublication>("maven") {
            groupId = project.group.toString()
            artifactId = project.name
            version = project.version.toString()

            afterEvaluate {
                artifact(prefabAar) {
                    extension = "aar"
                }

                artifact(sourcesJar) {
                    classifier = "sources"
                }
            }

            pom {
                name.set(project.name)
                description.set(project.description)
                url.set("https://github.com/powersync-ja/powersync-sqlite-core")

                developers {
                    developer {
                        id.set("journeyapps")
                        name.set("Journey Mobile, Inc.")
                        email.set("info@journeyapps.com")
                    }
                }

                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }

                scm {
                    connection.set("scm:git:github.com/powersync-ja/powersync-sqlite-core.git")
                    developerConnection.set("scm:git:ssh://github.com/powersync-ja/powersync-sqlite-core.git")
                    url.set("https://github.com/powersync-ja/powersync-sqlite-core")
                }
            }
        }
    }

    repositories {
        maven {
            name = "here"
            url = localRepo
        }
    }
}

signing {
    if (System.getenv("GPG_PRIVATE_KEY") == null) {
        // Don't sign the publication.
    } else {
        var signingKey = String(Base64.getDecoder().decode(System.getenv("GPG_PRIVATE_KEY"))).trim()
        var signingPassword = System.getenv("GPG_PASSWORD")
        useInMemoryPgpKeys(signingKey, signingPassword)

        sign(publishing.publications)
    }
}

tasks.withType<AbstractPublishToMaven>() {
    dependsOn(prefabAar)
}

val zipPublication by tasks.registering(Zip::class) {
    dependsOn(tasks.named("publishAllPublicationsToHereRepository"))

    archiveFileName.set("powersync_android.zip")
    from(localRepo)
}

tasks.named("build") {
    dependsOn(prefabAar)
}
