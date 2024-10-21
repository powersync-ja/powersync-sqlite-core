import java.util.Base64

plugins {
    id("maven-publish")
    id("signing")
}

group = "co.powersync"
version = "0.3.4"
description = "PowerSync Core SQLite Extension"

repositories {
    mavenCentral()
    google()
}

val buildRust = tasks.register<Exec>("buildRust") {
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
        if (System.getenv("OSSRH_USERNAME") != null) {
            maven {
                name = "sonatype"
                url = uri("https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/")
                credentials {
                    username = System.getenv("OSSRH_USERNAME")
                    password = System.getenv("OSSRH_PASSWORD")
                }
            }
        }

        if (System.getenv("GITHUB_ACTOR") != null) {
            maven {
                name = "GitHubPackages"
                url = uri("https://maven.pkg.github.com/powersync-ja/powersync-sqlite-core")
                credentials {
                    username = System.getenv("GITHUB_ACTOR")
                    password = System.getenv("GITHUB_TOKEN")
                }
            }
        }
    }
}

signing {
    if (System.getenv("GPG_PRIVATE_KEY") == null) {
        useGpgCmd()
    } else {
        var signingKey = String(Base64.getDecoder().decode(System.getenv("GPG_PRIVATE_KEY"))).trim()
        var signingPassword = System.getenv("GPG_PASSWORD")
        useInMemoryPgpKeys(signingKey, signingPassword)
    }
    sign(publishing.publications)
}

tasks.withType<AbstractPublishToMaven>() {
    dependsOn(prefabAar)
}

tasks.named("build") {
    dependsOn(prefabAar)
}
