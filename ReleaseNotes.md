
# Moqui ElasticSearch Release Notes

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
