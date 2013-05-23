#!/usr/bin/ruby
#This is Row CDR importer module which parse Teles Row CDR purly and push them into DB
require 'rubygems'
require 'mongo'
require 'bson'
#require 'trie'
#require 'awesome_print'
include Mongo
@database = MongoClient.new('localhost').db("local")
@coll = @database.collection("row")
#################
def extractElements(legX)
doc = {"stage" => 0}
  legX.gsub(/(\w)\(/, "\n[\\1]\(").each_line do |row|
    case row[1]
	when "I" then doc [:Ingress] = row
	when "O" then doc [:Egress] = row
	when "B" then doc [:elemB] = row
	when "C" then doc [:elemC] = row
	when "U" then doc [:elemU] = row
	when "V" then doc [:elemV] = row
	when "A" then doc [:elemA] = row
	when "D" then doc [:elemD] = row
	when "M" then doc [:elemM] = row
	when "E" then doc [:elemE] = row
	when "Y" then doc [:elemY] = row
	when "Z" then doc [:elemZ] = row
	else
	  if row.length > 1
	    puts row  #doc = {"elemMisc" => "#{row}"} ; coll.update({"_id" => @id},{"$set" => doc})
	  end
	end
    end
@coll.insert(doc)
#puts id
end
##########
#reader = `sed 's:,,*:,:g' #{ARGV[0]} | tr -d '\n' | sed 's:I(:\\nI(:g' | sed 's:O(:\\nO(:g'`
reader = `cat #{ARGV[0]} | tr -d '\n' | sed -e 's:[IOYZ](:\\n&:g'`
reader.each_line do |row|
case row[0]
    when "I" then extractElements row  # ;puts row
    when "O" then extractElements row  # ;puts row
    when "Y" then extractElements row  # ;puts row
    when "Z" then extractElements row  # ;puts row
    else puts "Misc:\n#{row}\n======="
  end
end
