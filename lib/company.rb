require 'jsonb_accessor'

class Company < ActiveRecord::Base
  jsonb_accessor(
    :data,
    companyForm: :string
  )

  def registration_date
    date = data['registrationDate']
    return unless date
    Date.parse(data['registrationDate'])
  end

  # def company_form
  #   data['companyForm']
  # end



end
