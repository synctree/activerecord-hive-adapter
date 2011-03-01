# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "activerecord-hive-adapter/version"

Gem::Specification.new do |s|
  s.name        = "activerecord-hive-adapter"
  s.version     = ActiveRecord::ConnectionAdapters::HiveAdapter::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Masahji Stewart", "Blake Petetan"]
  s.email       = ["masahji@synctree.com", "blake@synctree.com"]
  s.homepage    = "http://synctree.com"
  s.summary     = "ActiveRecord connection adapter for Hive"
  s.description = "ActiveRecord connection adapter for Hive"

  s.add_development_dependency 'rspec', '~> 1.3.1'

  s.add_runtime_dependency 'rbhive', '~> 0.1'
  
  s.rubyforge_project = "activerecord-hive-adapter"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
end
