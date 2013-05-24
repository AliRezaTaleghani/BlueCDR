#!/usr/bin/ruby
#This is Row CDR Madiator which will try to look call legs up and generating Call-CDR
require 'rubygems'
require 'mongo'
require 'bson'
require 'peach'
require 'redis'
require 'awesome_print'
include Mongo
@database = MongoClient.new('localhost',27017,:pool_size => 1, :pool_timeout => 180).db("local")
@coll = @database.collection("row")
cursor=@coll.find({"Ingress" => {'$exists'=>1}})
  cursor.each do |rowIn|
    callid = rowIn['Ingress'].split(',').to_a[1]
    engine = rowIn['Ingress'].split(',').to_a[7]
	puts "CallID(Ingress): #{callid}"
######################################
## Egress lookup query
        cmd = {
        aggregate: 'row',
	pipeline:[
		{'$match' => {'Egress' => {'$regex' => callid } }},
#		{'$project' => {'Egress' => 1}},
            ]
            }
	res = @database.command(cmd)['result']
######################################
	res.each do |rowEg|
		puts "CallID(Egress): #{rowEg['Egress'].split(',').to_a[1]}"
		puts "\t#{rowIn['_id']}\n\t#{rowEg['_id']}"
		puts rowEg['elemA'].split('{').to_a[1].split(',').to_a[0]
		puts rowEg['elemA'].split('{').to_a[2].split(',').to_a[18]
		puts rowEg['elemA'].split('{').to_a[2].split(',').to_a[20]
		puts rowEg['elemA'].split('{').to_a[2].split(',').to_a[45]
		puts rowEg['elemC']#.split(',').to_a[3]
	end

	puts "Engine: #{engine}"
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
