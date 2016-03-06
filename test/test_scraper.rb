require 'test_helper'
require 'scraper'
require 'vcr'


VCR.configure do |config|
  config.cassette_library_dir = 'fixtures/vcr_cassettes'
  config.hook_into :webmock # or :fakeweb
end

describe Scraper do
  before do
    @scraper = Scraper.new
  end

  it 'works' do
    VCR.use_cassette('prh', record: :new_episodes) do
      @scraper.foo
    end
  end

end
