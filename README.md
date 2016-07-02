# Moqui ElasticSearch Tool Component

[![license](http://img.shields.io/badge/license-CC0%201.0%20Universal-blue.svg)](https://github.com/moqui/moqui-elasticsearch/blob/master/LICENSE.md)
[![release](http://img.shields.io/github/release/moqui/moqui-elasticsearch.svg)](https://github.com/moqui/moqui-elasticsearch/releases)

Moqui Tool Component for ElasticSearch. Useful for scalable faceted text search, and analytics and reporting using
aggregations and other great features.

To install run (with moqui-framework):

    $ ./gradlew getComponent -Pcomponent=moqui-elasticsearch

This will add the component to the Moqui runtime/component directory.

The ElasticSearch, Apache Lucene, and dependent JAR files are added to the lib directory when the build is run for this component, which is
designed to be done from the Moqui build (ie from the moqui root directory) along with all other component builds.

To use just install this component, build, and load seed data. The configuration for the ToolFactory is already in place in the
MoquiConf.xml included in this component and will be merged with the main configuration at runtime. The seed data adds
the DataDocument screens to the System app in the tools component in the default moqui-runtime.
