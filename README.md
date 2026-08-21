The goal of SagaMQ is to provide a simple solution to build and run workflows with the necessary features to cover a wide range of processing needs.

To keep the framework simple as well as fast and with minimal CPU and RAM consumption, the following choices were made :
- The framework is written in Go
- Workflows and associated runtime engine are stateless, meaning that workflow tasks are responsible for data persistency when necessary
- Any asynchronous communication between workflows relies on LavinMQ (chosen for its capacity to deliver messages with a delay when needed)
- Workflows are expected to be deployed as containers in a k8s cluster to benefit "out of the box" of security, scalability,  load balancing, failover, monitoring and other "enterprise class" features
- No user interaction is implemented
- There is no graphical representation of the workflows (and thus, no modeling tool of any kind)

What SagaMQ does provide :
- Each workflow has an associated context handed over to the tasks so that they can access and share data
- The context can be used to pass along a 'replay' flag so that each task gets informed that the workflow is being replayed (typically after an incident) and is given the possibility to act consequently
- The context can also be used to store a workflow version number or a "feature flipping" indicator of any kind

- A task can be defined with an associated retry policy that will be applied by the workflow engine if the task fails
- The framework comes along with two retry policies : exponential backoff or max number of retries along with an optional jitter

- The engine logs start and stop for the workflow and its tasks as well as any task failure and retries to feed k8s monitoring

- Finally, the framework also implements both a sender and a receiver to provide an optional and ready-to-use integration with LavinMQ
