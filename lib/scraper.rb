require 'net/http'
require 'json'
require 'active_record'
require 'company'
require 'date'

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

  def process_results2(results)
    results.each do |result|
      puts result['businessId']
      sleep 0.2
    end
  end

  def foo
    uri = URI('http://avoindata.prh.fi:80/bis/v1')
    end_date = Company.last.registration_date.to_s
    start_date = (end_date - 1.day).to_s
    params = { totalResults: false, maxResults: 1000, resultsFrom: 0, companyRegistrationFrom: '1800-01-01'}
    #params = { totalResults: true, maxResults: 1000, resultsFrom: 0, companyRegistrationFrom: start_date, companyRegistrationTo: end_date}
    uri.query = URI.encode_www_form(params)

    while params[:companyRegistrationTo] != Date.today.to_s
      res = Net::HTTP.get_response(uri)
      json = JSON.parse(res.body)
      process_results(json['results'])

      if json['nextResultsUri']
        uri = URI(json['nextResultsUri'])
      else
        start_date =  params[:companyRegistrationTo]
        end_date = (Date.parse(start_date) + 1.day).to_s
        uri = URI('http://avoindata.prh.fi:80/bis/v1')
        params = { totalResults: false, maxResults: 1000, resultsFrom: 0, companyRegistrationFrom: start_date, companyRegistrationTo: end_date}
        uri.query = URI.encode_www_form(params)
      end
    end

    puts 'exiting scraper'
  end
end

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), 'lib'))
Scraper.new.foo
