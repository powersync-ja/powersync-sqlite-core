import java.util.Properties

plugins {
    id("com.android.library") version "8.0.1"
    id("maven-publish")
    id("signing")
}

group = "co.powersync"
version = "0.1.3"
description = "PowerSync Core SQLite Extension"

repositories {
    mavenCentral()
    google()
}

val buildRust = tasks.register("buildRust", Exec::class.java) {
    System.out.println("foo")
    workingDir("..")
    commandLine("cargo", "ndk", "-t", "armeabi-v7a", "-t", "arm64-v8a", "-t", "x86", "-t", "x86_64", "-o", "./android/build/intermediates/jniLibs", "build", "--release", "-Zbuild-std", "-p", "powersync_loadable")
}

android {
    compileSdk = 33
    ndkVersion = "25.2.9519653"

    namespace = "co.powersync.sqlitecore"

    defaultConfig {
        minSdk = 24

        ndk {
            abiFilters += setOf("armeabi-v7a", "arm64-v8a", "x86", "x86_64")
        }
    }

    sourceSets {
        getByName("main") {
            jniLibs.srcDir("build/intermediates/jniLibs")
        }
    }

    buildTypes {
        release {
            isMinifyEnabled = false
        }
    }

    publishing {
        singleVariant("release") {
            withSourcesJar()
        }
    }
}
tasks.withType<JavaCompile> {
    dependsOn(buildRust)
}

val secretsFile = rootProject.file("local.properties")
val secretProperties = Properties()

if (secretsFile.exists()) {
    secretsFile.reader().use { secretProperties.load(it) }

    secretProperties.forEach { key, value ->
        if (key is String && key.startsWith("signing")) {
            ext[key] = value
        }
    }
}

publishing {
    publications {
        register<MavenPublication>("maven") {
            groupId = project.group.toString()
            artifactId = project.name
            version = project.version.toString()

            afterEvaluate {
                from(components["release"])
            }

            pom {
                name.set(project.name)
                description.set(project.description)
                url.set("https://github.com/journeyapps/powersync-sqlite-core")

                developers {
                    developer {
                        id.set("journeyapps")
                        name.set("Journey Mobile, Inc.")
                        email.set("info@journeyapps.com")
                    }
                }

                licenses {
                    license {
                        name.set("Proprietary")
                    }
                }

                scm {
                    connection.set("scm:git:github.com/journeyapps/powersync-sqlite-core.git")
                    developerConnection.set("scm:git:ssh://github.com/journeyapps/powersync-sqlite-core.git")
                    url.set("https://github.com/journeyapps/powersync-sqlite-core")
                }
            }
        }
    }

    repositories {
        maven {
            name = "sonatype"
            url = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
            credentials {
                username = secretProperties.getProperty("ossrhUsername")
                password = secretProperties.getProperty("ossrhPassword")
            }
        }

        maven {
            name = "here"
            url = uri("build/here/")
        }
    }
}

signing {
    useGpgCmd()
    sign(publishing.publications)
}

tasks.withType<AbstractPublishToMaven>() {
    dependsOn("assembleRelease")
}