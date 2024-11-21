import os
import random
from collections import defaultdict

class StreamingTriangles:
    def __init__(self, edge_reservoir_size, wedge_reservoir_size):
        self.edge_reservoir_size = edge_reservoir_size
        self.wedge_reservoir_size = wedge_reservoir_size
        self.edge_reservoir = []  # Reservoir for edges
        self.wedge_reservoir = []  # Reservoir for wedges
        self.is_closed = []  # Tracks whether a wedge is closed
        self.total_wedges = 0  # Total number of wedges formed
        self.adjacency_list = defaultdict(set)  # Adjacency list for the graph

    def update(self, edge):
        u, v = edge

        # Update adjacency list
        self.adjacency_list[u].add(v)
        self.adjacency_list[v].add(u)

        # Update wedges in the wedge reservoir that are closed by the new edge
        for i, wedge in enumerate(self.wedge_reservoir):
            x, y, z = wedge
            if (u == x and v == y) or (u == y and v == x) or (u == x and v == z) or (u == z and v == x) or (u == y and v == z) or (u == z and v == y):
                self.is_closed[i] = True

        # Reservoir sampling for edges
        if len(self.edge_reservoir) < self.edge_reservoir_size:
            self.edge_reservoir.append(edge)
        else:
            replace_index = random.randint(0, len(self.edge_reservoir))
            if replace_index < self.edge_reservoir_size:
                self.edge_reservoir[replace_index] = edge

        # Generate new wedges with the new edge
        new_wedges = []
        for existing_edge in self.edge_reservoir:
            x, y = existing_edge
            if u == x or u == y:
                third_vertex = y if u == x else x
                if third_vertex != v:
                    new_wedges.append((u, v, third_vertex))
            if v == x or v == y:
                third_vertex = y if v == x else x
                if third_vertex != u:
                    new_wedges.append((v, u, third_vertex))

        self.total_wedges += len(new_wedges)

        # Reservoir sampling for wedges
        for wedge in new_wedges:
            if len(self.wedge_reservoir) < self.wedge_reservoir_size:
                self.wedge_reservoir.append(wedge)
                self.is_closed.append(False)
            else:
                replace_index = random.randint(0, len(self.wedge_reservoir))
                if replace_index < self.wedge_reservoir_size:
                    self.wedge_reservoir[replace_index] = wedge
                    self.is_closed[replace_index] = False

    def estimate(self):
        closed_wedges = sum(self.is_closed)
        transitivity = 3 * closed_wedges / len(self.wedge_reservoir) if self.wedge_reservoir else 0
        estimated_triangles = transitivity * self.total_wedges / 3 if self.total_wedges > 0 else 0
        return transitivity, estimated_triangles

def read_edges(file_path):
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
    except PermissionError:
        print(f"Error: Permission denied while trying to access {file_path}.")
        return []
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return []
    except Exception as e:
        print(f"Unexpected error while reading {file_path}: {e}")
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
    streaming_algo = StreamingTriangles(edge_reservoir_size=1000, wedge_reservoir_size=1000)

    # Simulate the streaming process
    print("Processing edges...")
    for edge in edges:
        streaming_algo.update(edge)

    # Get the estimates
    transitivity, triangles = streaming_algo.estimate()
    print(f"Estimated Transitivity: {transitivity}")
    print(f"Estimated Triangles: {triangles}")
