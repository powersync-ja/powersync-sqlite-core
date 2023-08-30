Pod::Spec.new do |s|
  s.name             = 'powersync-sqlite'
  s.version          = '0.1.0'
  s.summary          = 'PowerSync SQLite Extension'
  s.description      = <<-DESC
PowerSync extension for SQLite.
                       DESC

  s.homepage         = 'https://github.com/journeyapps/powersync-sqlite'
  s.license          = { :type => 'Commercial', :file => 'LICENSE' }
  s.author           = 'Journey Mobile, Inc'

  s.source   = { :http => "https://github.com/journeyapps/powersync-sqlite-core/releases/download/#{s.version}/powersync-sqlite.tar.xz" }
  s.vendored_frameworks  = 'powersync-sqlite-core.xcframework'

  s.ios.deployment_target = '10.0'
  s.osx.deployment_target = '10.10'
end
