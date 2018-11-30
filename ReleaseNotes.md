
# Moqui ElasticSearch Release Notes

## Release 1.2.0 - 29 Nov 2018

Moqui ElasticSearch 1.2.0 is a minor new feature and bug fix release.

This integration now uses ElasticSearch 6.3.2. There were significant changes in ElasticSearch in version 6 that are handled in 
this updated integration. Code that uses ElasticSearch directly may need to be updated, whereas code only using the index, search, 
and other services from this component should work fine without changes. 

By setting elasticsearch_mode to 'rest' and specifying at least elasticsearch_host1 the integration can now run without an embedded 
node and against a remote cluster via the ElasticSearch REST API. Before this the recommended production approach when an external
cluster was used was to have a processing only (no persistence) node running embedded in Moqui that joins the cluster. That can 
still be done and is more efficient but increases load on app servers. The main reason for using the REST API mode is for 
deployments such as on AWS ElasticSearch where joining a cluster is not possible.   

On startup if a DataFeed has indexOnStartEmpty=Y and ES indexes for the feed do not exist the full feed will be indexed so that 
indexes are populated based on relational database data for expected system operation.

## Release 1.1.0 - 22 Oct 2017

Moqui ElasticSearch 1.1.0 is a minor new feature and bug fix release.

ElasticSearch is now available to other tools on port 9200 bound to site local addresses, ie only localhost and other servers on 
the local internal network such as other docker containers on the same network.
 
ElasticSearch is available remotely through the main Moqui web server with a transparent proxy servlet on /elastic that requires
authentication and the 'ElasticRemote' permission. This can be done with HTTP Basic authentication per request or in an 
authenticated session.  

Similarly if Kibana is deployed on the same server, network, or docker host as the Moqui server it can be proxied through /kibana. 
The default setup uses Kibana running on localhost and the Kibana host can be specified using an environment property or by 
modifying the MoquiConf.xml file in this component. The best way to authenticate is login to the Moqui with a user that has the
'KibanaRemote' permission to establish a session and then go to /kibana.
