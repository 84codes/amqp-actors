task default: [:test]

task :test do
  if ENV['CI']
    require 'simplecov'
    SimpleCov.start
  end
  Dir.glob('./spec/**/*_spec.rb').each { |file| require file }
end
