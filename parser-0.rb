#!/usr/bin/ruby
#This is Row CDR importer module which parse Teles Row CDR purly and push them into DB
require 'rubygems'
require 'mongo'
require 'bson'
#require 'trie'
require 'date'
require 'peach'
require 'time'
require 'tzinfo'
#require 'tzinfo'
require 'awesome_print'
include Mongo
@database = MongoClient.new('localhost',27017,:pool_size => 1, :pool_timeout => 180).db("local")
@coll = @database.collection("row")
#################
def extractElements(legX)
#### initiate doc
doc = {"stage" => 0}
subDoc = {"stage" => 0}
### add row elemets to doc
  legX.gsub(/(\w)\(/, "\n[\\1]\(").each_line do |row|
    case row[1]
# In/Out Legs
	when "I" then doc ['Ingress'] = row
	when "O" then doc ['Egress'] = row
## elements
	when "A" then doc ['Connecting'] = row
	when "B" then doc ['Alerting'] = row
	when "C" then doc ['Connected'] = row
	when "U" then doc ['Accounting'] = row
	when "V" then doc ['VoIPandRadius'] = row
	when "D" then doc ['Disconnecting'] = row
	when "E" then doc ['Disconnected'] = row
	when "N" then doc ['NewSYStime'] = row
	when "M" then doc ['MediaGateway'] = row
# Special Events
	when "Y" then doc ['InitialSessionEnd'] = row
	when "Z" then doc ['SeriesEnd'] = row
	when "R" then doc ['Restart'] = row
	when "T" then doc ['SystemClock'] = row
	else
	  if row.length > 1
	    puts row  #doc = {"elemMisc" => "#{row}"} ; coll.update({"_id" => @id},{"$set" => doc})
	  end
	end
#### add subDoc
### Element ID Contents (Arrays init from Ziro)
### Record Header I,O (RECORD_TYPE, SID, DATE-TIME±UTCOFFSET,CDR_VERSION.DAEMON, SET_ID, NAME, 0, DAEMON_START,CALL_LEG_ID, ISID 32, ICLID32, #MD5HASH32)
### Record Header Y32 (RECORD_TYPE, SID, DATE-TIME±UTCOFFSET,CDR_VERSION.DAEMON, SET_ID, NAME, 0, DAEMON_START,CALL_LEG_ID, ISID 32, ICLID_MAX32, #MD5HASH32)
### Record Header Z   (RECORD_TYPE, SID, DATE-TIME±UTCOFFSET,CDR_VERSION.DAEMON, SET_ID, NAME, 0, DAEMON_START,CALL_LEG_ID, 032, 032, #MD5HASH32)
### Record Header R,T (RECORD_TYPE, 0, DATE-TIME±UTCOFFSET32,CDR_VERSION32.DAEMON32, SET_ID32, NAME32, 032, DAEMON_START32,032, 032, 032, #MD5HASH32)
	case row[1]
	  when "Y" then
		subDoc ["SID"] = doc['InitialSessionEnd'].split(',').to_a[1]
		subDoc ["DATETIMEGMT"] = (DateTime.strptime( doc['InitialSessionEnd'].split(',').to_a[2].split('+').to_a[0] ,'%d.%m.%Y-%H:%M:%S:%N') - Rational(doc['InitialSessionEnd'].split(',').to_a[2][-5,5],86400)).to_time.iso8601
		subDoc ["SET_ID"] = doc['InitialSessionEnd'].split(',').to_a[4]
		subDoc ["NAME"] = doc['InitialSessionEnd'].split(',').to_a[5]
		subDoc ["DAEMON_START"] = doc['InitialSessionEnd'].split(',').to_a[7]
		subDoc ["CALL_LEG_ID"] = doc['InitialSessionEnd'].split(',').to_a[8]
		subDoc ["ISID"] = doc['InitialSessionEnd'].split(',').to_a[9]
		subDoc ["ICLID_MAX"] = doc['InitialSessionEnd'].split(',').to_a[10]
	  when "Z" then
		subDoc ["SID"] = doc['SeriesEnd'].split(',').to_a[1]
		subDoc ["DATETIMEGMT"] = (DateTime.strptime( doc['SeriesEnd'].split(',').to_a[2].split('+').to_a[0] ,'%d.%m.%Y-%H:%M:%S:%N') - Rational(doc['SeriesEnd'].split(',').to_a[2][-5,5],86400)).to_time.iso8601
		subDoc ["SET_ID"] = doc['SeriesEnd'].split(',').to_a[4]
		subDoc ["NAME"] = doc['SeriesEnd'].split(',').to_a[5]
		subDoc ["DAEMON_START"] = doc['SeriesEnd'].split(',').to_a[7]
		subDoc ["CALL_LEG_ID"] = doc['SeriesEnd'].split(',').to_a[8]
		subDoc ["ISID"] = doc['SeriesEnd'].split(',').to_a[9]
	  when "I" then
		subDoc ["SID"] = doc['Ingress'].split(',').to_a[1]
		subDoc ["DATETIMEGMT"] = (DateTime.strptime( doc['Ingress'].split(',').to_a[2].split('+').to_a[0] ,'%d.%m.%Y-%H:%M:%S:%N') - Rational(doc['Ingress'].split(',').to_a[2][-5,5],86400)).to_time.iso8601
		subDoc ["SET_ID"] = doc['Ingress'].split(',').to_a[4]
		subDoc ["NAME"] = doc['Ingress'].split(',').to_a[5]
		subDoc ["DAEMON_START"] = doc['Ingress'].split(',').to_a[7]
		subDoc ["CALL_LEG_ID"] = doc['Ingress'].split(',').to_a[8]
		subDoc ["ISID"] = doc['Ingress'].split(',').to_a[9]
		subDoc ["ICLID"] = doc['Ingress'].split(',').to_a[10]

	  when "O" then
		subDoc ["SID"] = doc['Egress'].split(',').to_a[1]
		subDoc ["DATETIMEGMT"] = (DateTime.strptime( doc['Egress'].split(',').to_a[2].split('+').to_a[0] ,'%d.%m.%Y-%H:%M:%S:%N') - Rational(doc['Egress'].split(',').to_a[2][-5,5],86400)).to_time.iso8601
		subDoc ["SET_ID"] = doc['Egress'].split(',').to_a[4]
		subDoc ["NAME"] = doc['Egress'].split(',').to_a[5]
		subDoc ["DAEMON_START"] = doc['Egress'].split(',').to_a[7]
		subDoc ["CALL_LEG_ID"] = doc['Egress'].split(',').to_a[8]
		subDoc ["ISID"] = doc['Egress'].split(',').to_a[9]
		subDoc ["ICLID"] = doc['Egress'].split(',').to_a[10]
	end
### add SubDoc		
	doc["summery"] = subDoc
#	ap doc
###############
    end
@coll.insert(doc)
end
##########
reader = `cat #{ARGV[0]} | tr -d '\n' | sed -e 's:[IOYZRT](:\\n&:g'`
reader.each_line do |row|
case row[0]
    when "I" then extractElements row  # ;puts row
    when "O" then extractElements row  # ;puts row
    when "Y" then extractElements row  # ;puts row
    when "Z" then extractElements row  # ;puts row
    when "R" then extractElements row  # ;puts row
    when "T" then extractElements row  # ;puts row
   #else puts "Misc:\n#{row}\n======="
  end
end
