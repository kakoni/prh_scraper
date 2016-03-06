require 'minitest/spec'
require 'minitest/autorun'
require 'minitest/reporters'
require 'byebug'

Minitest::Reporters.use! Minitest::Reporters::SpecReporter.new # spec-like progress
