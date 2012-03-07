Gem::Specification.new do |s|
 s.name        = 'partitioned'
 s.version     = '0.8.0'
 s.date        = '2012-03-07'
 s.summary     = "Postgres table partitioning support for ActiveRecord."
 s.description = "A gem providing support for table partitioning in ActiveRecord.  Support is currently only supported for postgres database.  Other features include child table management (creation and deletion) abd bulk data creating and updating"
 s.authors     = ["Keith Gabryelski", "Aleksandr Dembskiy"]
 s.email       = 'keith@fiksu.com'
 s.files       = `git ls-files`.split("\n")
 s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
 s.require_path = 'lib'
 s.homepage    = 'http://www.fiksu.com'
end
