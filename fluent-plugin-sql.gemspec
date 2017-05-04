# encoding: utf-8
$:.push File.expand_path('../lib', __FILE__)

Gem::Specification.new do |gem|
  gem.name        = "fluent-dit-plugin"
  gem.description = "SQL input/Kinesis output plugin for Fluentd event collector"
  gem.homepage    = "https://github.com/dxdung/fluent-dit-plugin"
  gem.summary     = gem.description
  gem.version     = File.read("VERSION").strip
  gem.authors     = ["Dung Dinh"]
  gem.email       = "dxdung@gmail.com"
  gem.has_rdoc    = false
  #gem.platform    = Gem::Platform::RUBY
  gem.files       = `git ls-files`.split("\n")
  gem.test_files  = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.executables = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.require_paths = ['lib']
  gem.license = "Apache-2.0"

  gem.add_dependency "fluentd", [">= 0.12.17", "< 2"]
  gem.add_dependency 'activerecord', "~> 4.2"
  gem.add_dependency 'activerecord-import', "~> 0.7"
  gem.add_development_dependency "rake", ">= 0.9.2"
  gem.add_development_dependency "pg"
  gem.add_development_dependency "aws-sdk"
  gem.add_development_dependency "logger"
end
