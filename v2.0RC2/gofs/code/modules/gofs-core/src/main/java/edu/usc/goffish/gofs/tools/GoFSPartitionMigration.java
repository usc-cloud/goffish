package edu.usc.goffish.gofs.tools;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.ws.rs.core.UriBuilder;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;

import edu.usc.goffish.gofs.IPartitionDirectory;
import edu.usc.goffish.gofs.namenode.DataNode;
import edu.usc.goffish.gofs.namenode.IInternalNameNode;
import edu.usc.goffish.gofs.namenode.NameNodeProvider;
import edu.usc.goffish.gofs.util.SCPHelper;

public class GoFSPartitionMigration {

	private static final String GOFS_ROOT_DIR = "gofs/";

	public static void main(String[] args) throws IOException {
		if(args.length < 10){
			printUsageAndExit(null);
		}

		String graphId = null;
		int partitionId = -1;
		URI migrationFileUri = null, destNameNodeUri = null;
		String nameNodeType = null;
		boolean clusterFlag = false;
		String sourceNameNodeType = null;
		URI sourceNameNodeURI = null;
		for(int i=0; i<args.length; i++){
			argLoop: switch(args[i]){
			case "-graphid":
				graphId = args[++i];
				break;
			case "-partitionid":
				partitionId = Integer.parseInt(args[++i]);
				break;
			case "-cluster":
				clusterFlag = true;
				String[] sourceNameNodeParams = args[++i].split(" ");
				if(sourceNameNodeParams.length < 2){
					throw new IllegalArgumentException("source namenodetype and namenodeuri should be specified.");
				}else{
					sourceNameNodeType = sourceNameNodeParams[0];
					sourceNameNodeURI = URI.create(sourceNameNodeParams[1]);
				}
				break;
			case "-copyTo":
				migrationFileUri = URI.create(args[++i]);
				break;
			case "-namenodetype":
				nameNodeType = args[++i];
				break;
			case "-namenodeuri":
				destNameNodeUri = URI.create(args[++i]);
				break;
			default:
				break argLoop;
			}
		}

		// retrieve name node
		IInternalNameNode destnameNode = loadNameNode(nameNodeType, destNameNodeUri);
		IPartitionDirectory destPartitionDirectory = destnameNode.getPartitionDirectory();
	
		//Read the partition mapping from name node
		URI partitionMapping;
		if(clusterFlag){
			IInternalNameNode sourcenamenode = loadNameNode(sourceNameNodeType, sourceNameNodeURI);
			partitionMapping = sourcenamenode.getPartitionDirectory().getPartitionMapping(graphId, partitionId);
		}else{
			partitionMapping = destPartitionDirectory.getPartitionMapping(graphId, partitionId);
		}
		
		//validate the partition mapping
		if(partitionMapping == null){
			throw new IllegalArgumentException("name node does not match partition mapping for graphid and partitionid.");
		}

		Path workingDir = null, deployDirectory, gofsConfigPath;
		Path partitionFilePath = Paths.get(partitionMapping.getPath());
		URI migrationGoFSRootUri;
		PropertiesConfiguration config = new PropertiesConfiguration();
		config.setDelimiterParsingDisabled(true);
		try {
			config.load(Files.newInputStream(partitionFilePath.resolve(DataNode.DATANODE_CONFIG)));

			config.setProperty(DataNode.DATANODE_NAMENODE_TYPE_KEY, nameNodeType);
			config.setProperty(DataNode.DATANODE_NAMENODE_LOCATION_KEY, destNameNodeUri);

			String remoteHost = migrationFileUri.getHost();
			String user = migrationFileUri.getUserInfo();
			migrationGoFSRootUri = new URI("file", user != null ? user : "", remoteHost != null ? remoteHost : "", -1, new StringBuffer(migrationFileUri.getPath())
			.append(java.io.File.separatorChar).append("gofs").append(java.io.File.separatorChar).toString(), null, null);
			config.setProperty(DataNode.DATANODE_LOCALHOSTURI_KEY, migrationGoFSRootUri);

			workingDir = Files.createTempDirectory("gofs_migrate");
			// create deploy directory
			deployDirectory = Files.createDirectory(workingDir.resolve(GOFS_ROOT_DIR));

			gofsConfigPath = Files.createFile(deployDirectory.resolve(DataNode.DATANODE_CONFIG));
			config.save(gofsConfigPath.toFile());

			System.out.println("Copying the slices and gofs.config...");
			//create an empty slices directory
			Files.createDirectory(deployDirectory.resolve(DataNode.DATANODE_SLICE_DIR));
			//scp the gofs.config from temp directory
			SCPHelper.SCP(deployDirectory, migrationFileUri);
			//scp the slices from partition directory
			SCPHelper.SCP(partitionFilePath.resolve(DataNode.DATANODE_SLICE_DIR), UriBuilder.fromUri(migrationFileUri).path("gofs").build());
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			if(workingDir != null)
				FileUtils.deleteQuietly(workingDir.toFile());
		}

		//update the partition mapping in name node
		System.out.println("Updating the partition mapping in name node for graph id: " + graphId + " and " + "partition id: " + partitionId);
		destPartitionDirectory.putPartitionMapping(graphId, partitionId, UriBuilder.fromUri(migrationGoFSRootUri).fragment(partitionMapping.getFragment()).build());
	}

	private static IInternalNameNode loadNameNode(String nameNodeType, URI nameNodeUri)
			throws IOException {
		IInternalNameNode nameNode;
		try {
			nameNode = NameNodeProvider.loadNameNode(nameNodeType, nameNodeUri);
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException("Unable to load name node", e);
		}

		System.out.println("Contacting name node for " + nameNodeUri + " uri..");

		// validate name node
		if (!nameNode.isAvailable()) {
			throw new IOException("Name node at " + nameNode.getURI() + " is not available");
		}

		return nameNode;
	}

	private static void printUsageAndExit(String error){
		if (error != null) {
			System.out.println("Error: " + error);
		}

		System.out.println("Usage:");
		System.out.println("   GoFSPartitionMigration -graphid <graphid> -partitionid <partitionid> " +
				"-copyTo <migrationuri> -namenodetype <namenodetype> -namenodeuri <namenodeuri> -cluster \"<sourcenamenodetype> <sourcenamenodeuri>\"");
		System.out.println("   The -graphid is an user supplied string used to distinguish the graph from");
		System.out.println("   others in the name node");
		System.out.println("   The -partitionid refers to the partition in name node which will be migrated to the");
		System.out.println("   destination location");
		System.out.println("   The -copyTo flag refers to the location to which the partition will be copied");
		System.out.println("   The -namenodetype is a fully qualified Java class representing the name");
		System.out.println("    node type");
		System.out.println("   The -namenodeuri is a uri representing the name node location");
		System.out.println("   The -cluster flag indicates the partition will be copied to another machine. Passwordless");
		System.out.println("   shell access should be established between the machines. If cluster flag is specified,");
		System.out.println("   partition mapping will be retrieved from the source name node.");
		
		System.exit(1);
	}
}
