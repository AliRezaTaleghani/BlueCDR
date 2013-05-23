#!/usr/bin/ruby
#This is Row CDR Madiator which will try to look call legs up and generating Call-CDR
require 'rubygems'
require 'mongo'
require 'bson'
require 'peach'
require 'redis'
require 'awesome_print'
include Mongo
@database = MongoClient.new('localhost',27017,:pool_size => 30, :pool_timeout => 180).db("local")
@coll = @database.collection("row")
cursor=@coll.find({"Ingress" => {'$exists'=>1}})
  cursor.peach do |row|
    callid = row['Ingress'].split(',').to_a[1]
    engine = row['Ingress'].split(',').to_a[7]
	#puts "In: #{callid}"
    cmd = {
        aggregate: 'row',
	pipeline:[
		{'$project' => {'Egress' => 1}},
		{'$match' => {'Egress' => {'$regex' => callid , '$regex' => engine } }},
            ]
            }
	res = @database.command(cmd)['result']
	res.each do |row|
	#	puts row['Egress'].split(',').to_a[1]
	end

	#puts "Engine: #{engine}"
end

#puts @coll.find({"Egress"=>{'$exists'=>1}}).count()
#puts row['Ingress']
#  cmd = {
#    aggregate: 'carriers',
#    pipeline: [
#  {'$project' => {
#    'rates' => 1,
#    'Name' => 1}},
#  {'$unwind' => '$rates'},
#  {'$match' => {
#    'rates.Prefix' => "#{dnid[0,dnid.length-count]}",
#    'rates.Activation' => "2013-04-21"
#  }},
#  {'$limit' => 1 },
# ]}
# res = @db.command(cmd)['result']
######################################
#cursor=@coll.find({"Egress"=>{"$exists"=>1}},:fields => {accountcodes: { '$elemMatch' => { id: accountcode }}})
#cursor=@coll.find({"Egress"=>{"$exists"=>1}},:fields => {elemA:1,elemD:1})
#cursor=@coll.find({},:fields => {Ingress:1,Egress:1,elemA:1,elemD:1})
######################################
