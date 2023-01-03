## Scalable Processing of Dominance-Based Queries

### What is this project?
This is an exercise in the context of the subject “Decentralized Technologies”, that take place in the master’s degree program "Data and Web Science" of Aristotle University of Thessaloniki.

### Exercise description
Given a potentially large set of d-dimensional points, where each point is represented as a d-dimensional vector, we need to detect interesting points. The project is based on the concept of dominance. We say that a point p dominates another point q, when p is as good as q in all dimensions and it is strictly better in at least one dimension. We will assume that small values are preferable. For example, the point p(1, 2) dominates q(3, 4) since 1 < 3 and 2 < 4. Also, p(1, 2) dominates q(1, 3) since although they have the same x coordinate, the y coordinate of p is smaller than that of q. There are three different tasks you need to complete:
* **Task1**. Given a set of d-dimensional points, return the set of points that are not dominated. This is also known as the skyline set.
* **Task2**. Given a set of d-dimensional points, return the k points with the highest dominance score. The dominance score of a point p is defined as the total number of points dominated by p.
* **Task3**. Given a set of d-dimensional points, return the k points from the skyline with the highest dominance score.