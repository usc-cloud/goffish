Installation

Follow the steps to get the right development environment for developing Gephi plug-ins. The bootcamp contains the latest version of the Gephi Platform plus the examples. 

1. Download and install the latest version of Netbeans IDE.

2. Fork and checkout the latest version of the Gephi Plugins Bootcamp
git clone git@github.com:username/gephi-plugins-bootcamp.git

3. Start Netbeans and Open Project. The bootcamp is automatically recognized as a module suite.

4. Right click on Modules, add existing project. Select the dynamicgraphplayer in the viz/gephi-plugin directory.

4. Right click on the project and select 'Run'. This starts Gephi with all the plug-ins loaded.

5. Expand the list of modules and double-click on DynamicGraphPlayer to open and browse the sources.


Execution

1. Once gephi is started with the plugin, select DynamicGraphPlayer window tab
2. Click Load Template and load the graph template file (in the gofs/resources directory)
3. Click Load Instances and load one or more instance files
4. set the playback speed appropriately (for the given sample set it to 250)
5. Click Play
