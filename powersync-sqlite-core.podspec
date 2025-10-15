Pod::Spec.new do |s|
  s.name             = 'powersync-sqlite-core'
  s.version          = '0.4.7'
  s.summary          = 'PowerSync SQLite Extension'
  s.description      = <<-DESC
PowerSync extension for SQLite.
                       DESC

  s.homepage         = 'https://github.com/powersync-ja/powersync-sqlite-core'
  s.license          = 'Apache License, Version 2.0'
  s.author           = 'Journey Mobile, Inc.'

  s.source   = { :http => "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/v#{s.version}/powersync-sqlite-core.xcframework.zip" }
  s.vendored_frameworks  = 'powersync-sqlite-core.xcframework'

  s.ios.deployment_target = '11.0'
  s.osx.deployment_target = '10.13'
  s.watchos.deployment_target = '9.0'
end
