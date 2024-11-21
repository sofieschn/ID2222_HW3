import os
import random
import time
from collections import defaultdict

class StreamingTriangles:
    def __init__(self, se, sw):
        """
        se: Edge reservoir size
        sw: Wedge reservoir size
        """
        self.se = se  # Edge reservoir size
        self.sw = sw  # Wedge reservoir size
        self.edge_reservoir = []  # Reservoir for edges
        self.wedge_reservoir = []  # Reservoir for wedges
        self.is_closed = []  # Tracks whether a wedge is closed
        self.total_wedges = 0  # Total number of wedges formed
        self.adjacency_list = defaultdict(set)  # Adjacency list for the graph

    def update(self, edge):
        """
        Update procedure from the STREAMING-TRIANGLES algorithm.
        """
        u, v = edge

        # Update adjacency list
        self.adjacency_list[u].add(v)
        self.adjacency_list[v].add(u)

        # Check for closed wedges with the new edge
        for i, wedge in enumerate(self.wedge_reservoir):
            a, b = wedge
            if (a == u and v in self.adjacency_list[b]) or (b == u and v in self.adjacency_list[a]) or \
               (a == v and u in self.adjacency_list[b]) or (b == v and u in self.adjacency_list[a]):
                self.is_closed[i] = True

        # Reservoir sampling for edges
        if len(self.edge_reservoir) < self.se:
            self.edge_reservoir.append(edge)
        else:
            replace_index = random.randint(0, len(self.edge_reservoir))
            if replace_index < self.se:
                # Replace an edge in the reservoir
                self.edge_reservoir[replace_index] = edge

        # Add new wedges involving the new edge to the wedge reservoir
        new_wedges = []
        for node in self.adjacency_list[u]:
            if node != v:
                new_wedges.append((node, v))
        for node in self.adjacency_list[v]:
            if node != u:
                new_wedges.append((node, u))

        # Update total wedge count
        self.total_wedges += len(new_wedges)

        # Reservoir sampling for wedges
        for wedge in new_wedges:
            if len(self.wedge_reservoir) < self.sw:
                self.wedge_reservoir.append(wedge)
                self.is_closed.append(False)
            else:
                replace_index = random.randint(0, len(self.wedge_reservoir))
                if replace_index < self.sw:
                    self.wedge_reservoir[replace_index] = wedge
                    self.is_closed[replace_index] = False

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
    se = 1000  # Edge reservoir size
    sw = 1000  # Wedge reservoir size
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
