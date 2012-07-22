using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Configuration;

namespace UdacityCS215
{
  class Program
  {
    static int Main(string[] args)
    {
      if (args.Length < 1)
      {
        Console.Error.WriteLine("Usage: {0} file_name", System.AppDomain.CurrentDomain.FriendlyName);
        return -1;
      }

      ActorCentrality.Run(args[0]);
      return 0;
    }
  }

  public static class GraphExtensions
  {
    public static void AddEdge<T>(this Dictionary<T, HashSet<T>> graph, T node1, T node2)
    {
      if (graph.ContainsKey(node1))
      {
        graph[node1].Add(node2);
      }
      else
      {
        graph[node1] = new HashSet<T>(new T[] { node2 });
      }
      if (graph.ContainsKey(node2))
      {
        graph[node2].Add(node1);
      }
      else
      {
        graph[node2] = new HashSet<T>(new T[] { node1 });
      }
    }

    public static int[][] ToArray(this Dictionary<int, HashSet<int>> graph)
    {
      int[][] array = new int[graph.Count][];

      foreach (KeyValuePair<int, HashSet<int>> element in graph)
      {
        array[element.Key] = new int[element.Value.Count];
        int i = 0;
        foreach (int value in element.Value)
        {
          array[element.Key][i++] = value;
        }
      }

      return array;
    }

    public static double AverageCentrality(this Dictionary<int, HashSet<int>> graph, int node)
    {
      int[] distance = new int[graph.Count];
      distance[node] = 1;

      int[] frontier = new int[graph.Count];
      int frontierLeft = 0;
      int frontierRight = 1;
      int actors = 1;
      frontier[0] = node;

      Int64 sum = 0;
      while (frontierLeft < frontierRight)
      {
        int currentNode = frontier[frontierLeft++];
        int value = distance[currentNode] + 1;
        foreach (int successor in graph[currentNode])
        {
          if (distance[successor] == 0)
          {
            sum += (value - 1);
            distance[successor] = value;
            frontier[frontierRight++] = successor;
            actors++;
          }
        }
      }

      return (sum + 0.0) / actors;
    }

    public static double AverageCentrality(this int[][] graph, int node)
    {
      int[] distance = new int[graph.Length];
      distance[node] = 1;

      int[] frontier = new int[graph.Length];
      int frontierLeft = 0;
      int frontierRight = 1;
      int actors = 1;
      frontier[0] = node;

      int sum = 0;
      while (frontierLeft < frontierRight)
      {
        int currentNode = frontier[frontierLeft++];
        int value = distance[currentNode] + 1;
        foreach (int successor in graph[currentNode])
        {
          if (distance[successor] == 0)
          {
            sum += (value - 1);
            distance[successor] = value;
            frontier[frontierRight++] = successor;
            actors++;
          }
        }
      }

      return (sum + 0.0) / actors;
    }
  }

  class ActorCentrality
  {
    static public void Run(string fileName)
    {
      DateTime start = DateTime.Now;

      //Build a graph
      Dictionary<int, HashSet<int>> graph = new Dictionary<int, HashSet<int>>();
      Dictionary<string, int> actorIDs = new Dictionary<string, int>();
      Dictionary<string, int> movieIDs = new Dictionary<string, int>();
      Dictionary<int, string> idNames = new Dictionary<int, string>();

      int id = 0;
      using (StreamReader reader = new StreamReader(fileName))
      {
        string line;
        while ((line = reader.ReadLine()) != null)
        {
          string[] elements = line.Split('\t');
          string actor = elements[0];
          string movie = string.Format("{0}/{1}", elements[1], elements[2]);

          if (!actorIDs.ContainsKey(actor))
          {
            idNames[id] = actor;
            actorIDs[actor] = id++;
          }

          if (!movieIDs.ContainsKey(movie))
          {
            idNames[id] = movie;
            movieIDs[movie] = id++;
          }

          graph.AddEdge(actorIDs[actor], movieIDs[movie]);
        }
      }

      ParallelOptions options = new ParallelOptions();
      if (ConfigurationManager.AppSettings["maxDegreeOfParallelism"] != null)
      {
        options.MaxDegreeOfParallelism = int.Parse(ConfigurationManager.AppSettings["maxDegreeOfParallelism"]);
      }

      int[][] graphArray = graph.ToArray();
      double[] centrality = new double[graph.Count];
      int maxCentrality = 0;
      Parallel.ForEach(actorIDs, element =>
      {
        centrality[element.Value] = graphArray.AverageCentrality(element.Value);
      });
      TimeSpan runTime = DateTime.Now.Subtract(start);

      //a = datetime.datetime.now() 
      Console.Out.WriteLine("{0} - {1}", maxCentrality, runTime);

      Dictionary<int, double> centralityDictionary = new Dictionary<int, double>(actorIDs.Count);
      foreach(KeyValuePair<string, int> element in actorIDs)
      {
        centralityDictionary[element.Value] = centrality[element.Value];
      }

      int rank = 1;
      foreach (KeyValuePair<int, double> actorCentrality in centralityDictionary.OrderBy(element => element.Value))
      {
        Console.Out.WriteLine("rank {0}: {1} centrality = {2:0.0000000}", rank++, idNames[actorCentrality.Key], actorCentrality.Value);
        if (rank > 20)
        {
          break;
        }
      }
    }
  }
}
