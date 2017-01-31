# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'amqp_actors/version'

Gem::Specification.new do |spec|
  spec.name          = "amqp-actors"
  spec.version       = AmqpActors::VERSION
  spec.authors       = ["Anders Bälter", "Magnus Hörberg"]
  spec.email         = ["anders@84codes.com", "magnus@84codes.com"]

  spec.summary       = "Ruby actors on top of AMQP"
  spec.homepage      = "https://github.com/84codes/amqp-actors"

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = "n/a"
  else
    raise "RubyGems 2.0 or newer is required to protect against public gem pushes."
  end

  spec.files         = `git ls-files -z`.split("\x0")
    .reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "bin"
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.require_paths = %w(lib)

  spec.add_runtime_dependency "bunny"

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "minitest"
end
