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

    def update(self, edge):
        u, v = edge

        # Update wedges in the wedge reservoir that are closed by the new edge
        for i, wedge in enumerate(self.wedge_reservoir):
            if wedge[2] == edge:  # Check if this edge closes the wedge
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
            if u in existing_edge or v in existing_edge:
                third_vertex = existing_edge[0] if existing_edge[1] in (u, v) else existing_edge[1]
                if u != third_vertex and v != third_vertex:
                    new_wedges.append((u, v, third_vertex))

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


# Example testing of the implementation
if __name__ == "__main__":
    streaming_algo = StreamingTriangles(edge_reservoir_size=10, wedge_reservoir_size=10)
    test_edges = [
        (1, 2),
        (2, 3),
        (3, 1),
        (4, 5),
        (5, 6),
        (6, 4),
        (7, 8),
        (8, 9),
        (9, 7),
    ]

    # Simulate edge stream processing
    for edge in test_edges:
        streaming_algo.update(edge)

    # Get the estimates
    transitivity, triangles = streaming_algo.estimate()
    print(f"Estimated Transitivity: {transitivity}")
    print(f"Estimated Triangles: {triangles}")
