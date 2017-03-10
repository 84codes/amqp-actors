require 'bundler/setup'
require 'amqp_actors'
require 'amqp_actors/test_actor'
require 'minitest/autorun'
require 'minitest/reporters'
reporter_options = { color: true }
Minitest::Reporters.use! [Minitest::Reporters::DefaultReporter.new(reporter_options)]
