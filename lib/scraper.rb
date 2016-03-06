require 'net/http'
require 'json'
require 'active_record'
require 'company'

class Scraper

  def initialize
    @connection = ActiveRecord::Base.establish_connection(
      adapter: "postgresql",
      database: "prh"
    )
    ActiveRecord::Base.logger = Logger.new(STDERR)
    create_db unless ActiveRecord::Base.connection.table_exists? 'companies'
  end

  def create_db
    ActiveRecord::Schema.define do
      create_table :companies do |table|
        table.column :business_id, :string, null: false
        table.column :data, :jsonb, null: false, default: {}
      end
    end
  end

  def save_business(business_id, uri)
    uri = URI(uri)
    res = Net::HTTP.get_response(uri)
    json = JSON.parse(res.body)
    puts "storing #{business_id}"
    Company.create!(business_id: business_id, data: json['results'][0])
  end

  def process_results(results)
    results.each do |result|
      business_id = result['businessId']
      business_uri = result['detailsUri']
      save_business(business_id, business_uri)
      sleep 0.5
    end
  end

  def foo
    uri = URI('http://avoindata.prh.fi:80/bis/v1')
    #params = { totalResults: true, maxResults: 1000, resultsFrom: 0, companyRegistrationFrom: '1970-01-01'}
    params = { totalResults: true, maxResults: 10, resultsFrom: 250, companyRegistrationFrom: '2016-02-28'}
    uri.query = URI.encode_www_form(params)

    while true do
      puts "fetching: #{uri}"
      res = Net::HTTP.get_response(uri)
      json = JSON.parse(res.body)
      next_uri = json['nextResultsUri']
      process_results(json['results'])
      break unless next_uri
      uri = URI(next_uri)
    end

    puts 'exiting scraper'
  end

end
