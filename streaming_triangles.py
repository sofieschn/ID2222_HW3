import os
import random
import time
from collections import defaultdict

# the steaming graph algorithm estimates the transitivity and triangle
# counts using resevior sampling

class StreamingTriangles:

    def __init__(self, se, sw):
        """
        se: Edge reservoir size
        sw: Wedge reservoir size
        """
        self.se = se  # Edge reservoir size
        self.sw = sw  # Wedge reservoir size
        self.edge_reservoir = []  # Holds a fixed number of edges sampled from the stream.
        self.wedge_reservoir = []  # Stores a fixed number of wedges formed from the sampled edges.
        self.is_closed = []  # Tracks whether a wedge in wedge_reservoir forms a triangle.
        self.total_wedges = 0  # Tracks the total number of wedges formed, regardless of sampling.
        self.adjacency_list = defaultdict(set)  # A dictionary where each node maps to its set of neighbors, representing the graph.
        self.edge_count = 0  # Counter to track edges processed for testing 


    def update(self, edge):
        """
        Update procedure from the STREAMING-TRIANGLES algorithm.
        Use the adjacency list to check if (u,v) completes any existing wedge in the wedge_reservoir into a triangle.
        """
        # A tuple (u, v) representing a new edge in the stream
        u, v = edge
        # just test to se processing is working fine
        if self.edge_count < 20:
            print(f"Edge {self.edge_count + 1}: {edge}")
        self.edge_count += 1



        # Update adjacency list. This step ensures the graph structure is updated for efficient neighbor lookups.
        self.adjacency_list[u].add(v)
        self.adjacency_list[v].add(u)

        # Check for closed wedges with the new edge
        # for each wedge in the wegde reservoir, we check if (u,v) can make a triangle out of the wedge
        for i, wedge in enumerate(self.wedge_reservoir):
            a, b = wedge
            # we check if node u matches the first node of the wedge, if it does, we check if v connects to b. 
            # and we do this for each possible match between wedge a, b and new egde u, v. 
            if (a == u and v in self.adjacency_list[b]) or \
            (b == u and v in self.adjacency_list[a]) or \
            (a == v and u in self.adjacency_list[b]) or \
            (b == v and u in self.adjacency_list[a]):
                self.is_closed[i] = True # if we find a match, aka (u, v) completed wedge (a, b) to a triangle, we will set the status of this wedge to closed = true.

        # ---- here we want to add the edge to the reservoir for data stream checks (part 1) 

        # Reservoir sampling for edges
        if len(self.edge_reservoir) < self.se: # check if the reservoir size is smaller than the max size (1000)
            self.edge_reservoir.append(edge) # add this edge to reservoir
        else:
            # in case the reservoir is at max volume
            # select a random number within the reservoir (1-1000)
            replace_index = random.randint(0, len(self.edge_reservoir))
            if replace_index < self.se:
                # Replace the selected edge in the reservoir
                self.edge_reservoir[replace_index] = edge

        # Add new wedges involving the new edge to the wedge reservoir 
        new_wedges = []
        # Iterates over all neighbors of u
        for node in self.adjacency_list[u]:
            # that are not equal to v, we add to the wedges 
            if node != v:
                new_wedges.append((node, v)) 
        for node in self.adjacency_list[v]:
            if node != u:
                new_wedges.append((node, u))

        # Update total wedge count
        self.total_wedges += len(new_wedges)

        # Reservoir sampling for wedges
        for wedge in new_wedges:
            if len(self.wedge_reservoir) < self.sw: # check if the wedge reservoir is at max size
                self.wedge_reservoir.append(wedge) # add wedge to the wedge reservoir if there is room
                self.is_closed.append(False) # set status closed = false
            else:
                # in case the wedge reservoir is full we will randomly exchange a wedge for the new one
                replace_index = random.randint(0, len(self.wedge_reservoir))
                if replace_index < self.sw:
                    self.wedge_reservoir[replace_index] = wedge
                    self.is_closed[replace_index] = False


    # calculate the estimations of closed wedges, transitivity and triangles.
    # since the reservoir never will look the same during different runs of the algorithm, we can only give estimations. 
    def estimate(self):
        """
        Estimate transitivity and triangle counts.
        """
        closed_wedges = sum(self.is_closed)
        transitivity = (3 * closed_wedges / len(self.wedge_reservoir)) if self.wedge_reservoir else 0
        estimated_triangles = (transitivity * self.total_wedges / 3) if self.total_wedges > 0 else 0
        return transitivity, estimated_triangles


def read_edges(file_path):
    """
    Reads edges from a graph file.
    """
    edges = []
    try:
        with open(file_path, "r") as file:
            for line in file:
                if line.startswith("#"):
                    continue  # Skip comment lines
                parts = line.strip().split()
                if len(parts) == 2:
                    edges.append((int(parts[0]), int(parts[1])))
        return edges
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return []


if __name__ == "__main__":
    # Dynamically construct the file path relative to the script's location
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "data.txt")

    # Check if the file exists and is accessible
    if os.path.isfile(file_path) and os.access(file_path, os.R_OK):
        print(f"File '{file_path}' found and is accessible. Proceeding...")
        edges = read_edges(file_path)
        if not edges:
            print("No edges could be read. Exiting.")
            exit(1)
    else:
        print(f"Error: File '{file_path}' is not accessible or does not exist.")
        exit(1)

    # Initialize the streaming triangles algorithm
    se = 10000  # Edge reservoir size
    sw = 10000  # Wedge reservoir size
    streaming_algo = StreamingTriangles(se, sw)

    # Measure the start time
    start_time = time.time()

    # Simulate the streaming process
    print("Processing edges...")
    for edge in edges:
        streaming_algo.update(edge)

    # Measure the end time
    end_time = time.time()

    # Get the estimates
    transitivity, triangles = streaming_algo.estimate()
    print(f"Estimated Transitivity (κ_t): {transitivity}")
    print(f"Estimated Triangles (T_t): {triangles}")

    # Calculate and print the runtime
    runtime = end_time - start_time
    print(f"Runtime: {runtime:.4f} seconds")
