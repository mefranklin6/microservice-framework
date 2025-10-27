# Microservice Framework
This is the primary version of the microservice_framework.go file that is in use by microservices. This repo should help to consolidate updates to the shared framework. All microservices should try to stay up to date with the latest version of this file. This repository should be added to microservices as a submodule to facilitate pulling changes.

## File Updates
You can test changes to the framework locally if you are debugging and want to minimize commits to the primary framework file. However, before rolling out any changes to a microservice in production, this repository must be updated. All microservices must then pull the latest changes to the microservice-framework as soon as possible.

### Versions
Whenever a commit is made, please update the version comment at the top of the framework file.

##### Format: version X.Y.Z

For example: version 1.0.12

- X - Large version change. Only updated when the core structure of the framework is overhauled.
- Y - Medium version change. Updated when a new feature is added. For example, adding UDP or Telnet communications.
- Z - Small version change. Updated every time a commit is made. Fine for small bug fixes or log tweaks.

## Build Using a Local Copy of the Framework
To build locally, run 
<pre>
go mod edit -replace github.com/Dartmouth-OpenAV/microservice-framework=./source/microservice-framework 
</pre>
in the root folder of your project with the go.mod file so that Go references your local framework files.

## Build Using a Pull Request Version of the Framework
In the microservice repo you're testing, make sure you update the .gitmodule with the fork path
<pre>
[submodule "source/microservice-framework"]
    path = source/microservice-framework
    url = https://github.com/mefranklin6/microservice-framework.git
</pre>
Note: This is an actual pull request.  You'll need to specify the url of the one you're testing.

Then in the root of your repo, run:
<pre>
git submodule sync
git submodule update --remote --init
</pre>
