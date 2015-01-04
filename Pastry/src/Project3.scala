import akka.actor._
import scala.util.Random
import scala.Array._
import scala.collection.mutable.HashMap
import scala.collection.immutable.TreeMap
import scala.collection.immutable.List
import scala.util.control.Breaks._
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer

case class NodeDeliver(count: Int)
case object CreateNetwork
case object StartRouting
case class Route(destinationNodeId:String,numberOfHopsPassed:Int,pastryMessage:String,master:ActorRef)
case class InitializeNode(sourceNodeId:String,numberOfNodes:Int,nodesMap:HashMap[String,ActorRef],nodeIdList:Array[String],routingTable:Array[Array[String]],leafNodes:Array[String],neighbourNodes:Array[String])

object Project3 extends App 
{

		var numberOfNodes=args(0).toInt
		var numberOfRequests=args(1).toInt
		
		/*
		 * Create a master which takes the number of nodes and number of requests to be used
		 */
		val master = ActorSystem("MasterSystem").actorOf(Props(new Master(numberOfNodes,numberOfRequests)),"master"); 		  		  		
		master ! CreateNetwork
		
}

class Master(numberOfNodes:Int,numberOfRequests:Int) extends Actor
{
  
	var nodesMap = new HashMap[String,ActorRef]
	var nodeIdList = new Array[String](numberOfNodes)
	var sortedNodeList = List.empty[String]
	var totalHopCount = 0.0f
	var aggregate = 0.0f
	var averageHopCount = 0.0f
	
	def receive = {
    
	case CreateNetwork =>
	  	
		val system = ActorSystem("Pastry")
		var leafNodes= new Array[String](8)
		var neighbhourNodes=new Array[String](8)
		var routingTable=Array.ofDim[String](8,4)
	

		println("******************start pastry***************")
		/*
		 * Assign unique node identifier to each node: start
		 */
		
		var nodeCount =0
		var digitIndex = 0 // Since we assign 8 digit identifier
		var nodeId = ""
		var isNodeAdded=0
		 
		for(nodeCount<-0 until numberOfNodes by 1)
		{
			val nodeReference = system.actorOf(Props(new NodeActor(numberOfNodes)))
			isNodeAdded=0
			while(isNodeAdded == 0)
			{
				nodeId="" 
				digitIndex=0
				while(digitIndex!=8)
				{
					nodeId+=Random.nextInt(4).toString() 
					digitIndex+=1
				}
			
				if(!nodesMap.contains(nodeId))
				{
					nodesMap.put(nodeId,nodeReference)
					nodeIdList(nodeCount)=nodeId
					isNodeAdded=1
				}
			}
		}    
		      	
      	/*
		 * Assign unique node identifier to each node: end
		 */
      	
    	//Sort the node list to form the leaf nodes.
      	sortedNodeList=nodesMap.keySet.toList.sorted
   
    	var count = 0
		nodeId =""
		nodeCount = 0
		
		for(nodeCount<-0 until numberOfNodes by 1)
		{ 
			leafNodes= new Array[String](8)
			neighbhourNodes=new Array[String](8)
			routingTable=Array.ofDim[String](8,4)
			nodeId=nodeIdList(nodeCount)
					
			
			// Assign neighbor nodes start
			var nodeIndex = 0
			for(nodeIndex<-0 until 8 by 1)
			{  
			    neighbhourNodes(nodeIndex)=nodeIdList((nodeIndex+1)%numberOfNodes)
			}
			// Assign neighbor nodes send
			
			
			//Assign leaf nodes - Start
			// Includes rare cases like first 4 values and last 4 values
			
			nodeIndex=sortedNodeList.indexOf(nodeId)
			var loopCounter = 0
			var currentNodeIndex = 0
			
		  	if((nodeIndex-4)<0)
		  	{
		  		currentNodeIndex=0
		  	}
		  	else if((nodeIndex+4)>numberOfNodes-1)
		  	{
		  		currentNodeIndex=numberOfNodes-9
		  	}
		  	else
		  	{
		  		currentNodeIndex=nodeIndex-4
		  	}
			
			for(loopCounter<-0 until 8 by 1)
			{
				if(currentNodeIndex==nodeIndex)
				{
					currentNodeIndex+=1
				}
	
				leafNodes(loopCounter)=sortedNodeList(currentNodeIndex)
				currentNodeIndex=currentNodeIndex+1
			}

		//Assign leaf nodes - end
	
		
			
		//Construct the routing table
			
			var filledCount=0
			var flag:Boolean=false
			var column=0
			var digit: Char=' '
			digitIndex=0
			var isCellFilled = Array.ofDim[Boolean](8,4)
			
		  for((nodeIdentifier,nodeReference)<-nodesMap)
				{
			    
				 digitIndex=0
				 flag=false
				 column=0
				 if(!nodeIdentifier.equals(nodeId))
				 {
				   
				  while(!flag)
				  {
				  if(nodeIdentifier.charAt(digitIndex).toString.equals(nodeId.charAt(digitIndex).toString))
				  {
				    digitIndex+=1
				  }
				  else
				  {
				    column = nodeIdentifier.charAt(digitIndex).toString.toInt
				    if(!isCellFilled(digitIndex)(column))
				    {
				    	routingTable(digitIndex)(column) = nodeIdentifier
				    	isCellFilled(digitIndex)(column) = true
				    	flag=true
				    }
				    else
				    {
				      flag=true
				    }
				  }
				}
			}
		}
			
			nodesMap.apply(nodeId)! InitializeNode(nodeId,numberOfNodes,nodesMap,nodeIdList,routingTable,leafNodes,neighbhourNodes)
			
		}

      	println("*********Routing table constructed**************")
		self ! StartRouting
	
		/*
		 * Routing starts at this point
		 */
		
		case StartRouting =>
		
		  var k=0
		  var requestCounter = numberOfRequests
		  for(i<-0 until numberOfNodes by 1)
			{
				k=i
				while(requestCounter>0)
				{
					k=(k+1)%numberOfNodes
					nodesMap.apply(nodeIdList(i)) ! Route(nodeIdList(k),0,"Implement Pastry",self)
					requestCounter=requestCounter-1
				}
				requestCounter=numberOfRequests
			}
		  
		  case NodeDeliver(hopCount)=>
		  
		  	totalHopCount+=hopCount
			aggregate+=1
				  	
		  	if(aggregate==(numberOfRequests*numberOfNodes))
		  	{
		  			Thread.sleep(5000);
		  			printf("Average Hop count : %.2f", totalHopCount/aggregate)
					 System.exit(0)
		  	}
		}				
 }

class NodeActor(numberOfNodes:Int) extends Actor
{
  
  var sourceNodeId:String = ""
  var numberOfRequests = 0
  var nodesMap = new HashMap[String,ActorRef]
  var nodeIdList = new Array[String](numberOfNodes)
  var routingTable = Array.ofDim[String](8,4)
  var leafNodes  = new Array[String](8)
  var neighbourNodes = new Array[String](8)
  var hopCount = 0
    
  
  def receive = {
    
    case Route(destinationNodeId,numberOfHopsPassed,pastryMessage,master) =>
      
      	var nodeToBeForwarded = sourceNodeId
      	var unionSet = new Array[String](48)
      	var indexOfUnionSet = 0
      	var nextNode = ""
		var index=0
		var rowNumber = 0
		var columnNumber = 0
		var loopCounter = 0
		var shortestDistance = 0
		var destination = Integer.parseInt(destinationNodeId)
		var numberOfhopsPassed = numberOfHopsPassed%15
		var prefixMatchCurrentnode = 0
		var unionSetArray = new ArrayBuffer[String]
		var destinations = new ArrayBuffer[String]
      	var visited = false
       	hopCount = numberOfhopsPassed
      	if(destinations.contains(destinationNodeId))
      	{
      	  visited = true
      	}
      	else
      	{
      		destinations+=destinationNodeId
      	}
      	
		if(sourceNodeId.equals(destinationNodeId)) {
		  	
		  master ! NodeDeliver(hopCount)
		}
		
      	
      	//check the leaf nodes if the destination key is available in the list
      	if ((destination >= Integer.parseInt(leafNodes(0))) && (destination <= Integer.parseInt(leafNodes(7))))
      	{
      	  
      		shortestDistance = math.abs(destination-Integer.parseInt(leafNodes(0)))
      		
      		nodeToBeForwarded = leafNodes(0)
    		for(loopCounter<-1 until 8 by 1)
    		{
				if(math.abs(destination-Integer.parseInt(leafNodes(loopCounter))) < shortestDistance)
				{
					shortestDistance = math.abs(destination-Integer.parseInt(leafNodes(loopCounter)))
					nodeToBeForwarded = leafNodes(loopCounter)
				}
    		}
      	}
      	

      	
      	
      else  //using routing table
      {
        //find l value
	    breakable
		{
			for(loopCounter<-1 until 7 by 1)
			{
				if (sourceNodeId.substring(0,loopCounter) == destinationNodeId.substring(0,loopCounter))
				{
					prefixMatchCurrentnode = loopCounter
				}
				else
				{
					break
				}
			}
	    }
	
	rowNumber = prefixMatchCurrentnode 
	columnNumber = destinationNodeId.charAt(prefixMatchCurrentnode).toString.toInt
	
	if((routingTable(rowNumber)(columnNumber) != null)&&(prefixMatchCurrentnode>0))
	{
		nodeToBeForwarded = routingTable(rowNumber)(columnNumber)
	}
	else //rarecase
	{
	  	var indexTset=0
		//forming the set T
			
		unionSetArray.insertAll(0, neighbourNodes)
		unionSetArray.insertAll(neighbourNodes.length,leafNodes)
		index=0
		loopCounter=0
		var check=0
		indexOfUnionSet = 16
		while(check < 32)
		{
		  for(loopCounter<-0 until 4 by 1)
		  {
		    if(routingTable(index)(loopCounter) != null)
			{
		    	unionSetArray+= routingTable(index)(loopCounter)
				
			}
		    check+=1
		  }
		  index+=1
		}

		var prefixMatchDestination = -1
		index=0
		nodeToBeForwarded = sourceNodeId
		var flag=true
		var i=1
		while(index<unionSetArray.length-1)
		{
		  
		  if(unionSetArray(index)!=null)
		  {
		  	nextNode = unionSetArray(index)
			
				while((i<8) && (flag))
				{
					if (nextNode.substring(0,i) == destinationNodeId.substring(0,i))
					{
						prefixMatchDestination = i
					}
					else 
					{
						flag=false
					}
					i+=1
				}
			
			if(prefixMatchDestination>prefixMatchCurrentnode)
			{
			  prefixMatchCurrentnode = prefixMatchDestination
			  nodeToBeForwarded = nextNode
			}
			else 
			 {
			  if(prefixMatchDestination == prefixMatchCurrentnode)
			{
			  if(math.abs(Integer.parseInt(nextNode)-destination) < math.abs(Integer.parseInt(sourceNodeId)-destination))
			  {
				  prefixMatchCurrentnode = prefixMatchDestination
				  nodeToBeForwarded = nextNode
			  }
			}
		  }
			index+=1			
		  }
		}
	}
}

      	if(nodeToBeForwarded.equals(sourceNodeId))
      	{
      	   master ! NodeDeliver(hopCount)
      	}
      	else
      	{
      		
      		if(!visited)
      			nodesMap.apply(nodeToBeForwarded)! Route(destinationNodeId,hopCount+1,pastryMessage,master)
      		else
      			  nodesMap.apply(nodeToBeForwarded)! Route(destinationNodeId,hopCount,pastryMessage,master)
      	}
		
    case InitializeNode(nodeId,numberOfRequests,nodesMap,nodeIdList,routingTable,leafNodes,neighbourNodes) =>
	
	  	this.sourceNodeId=nodeId
		this.numberOfRequests=numberOfRequests
		this.nodesMap=nodesMap
		this.nodeIdList=nodeIdList
		this.routingTable =routingTable
		this.leafNodes=leafNodes
		this.neighbourNodes=neighbourNodes 
    
  }
  
}



