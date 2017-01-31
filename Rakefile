task default: [:test]

task :test do
  Dir.glob('./spec/**/*_spec.rb').each { |file| require file }
end
