Way to use the docker:

1. for building the docker file we need to execute the below command which will build the docker image named sparkhome.
    
   docker build . -t sparkhome

2. to list and see the build docker image run the ls commmand and see the list of docker images created.

   docker image ls

3. to run the docker image use the below command which will run the image and create a container called spark to run our pyspark code.

   docker run --platform linux/amd64 -p 8888:8888 --name spark -d sparkhome

  We can run this or in the container logs we can get the jypiter notebook access with pyspark sessions where we can just execute the code.

  docker logs spark (container name)