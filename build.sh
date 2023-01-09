#! /bin/bash
sbt clean assembly
mv target/scala-2.13/DominanceQueries-assembly-0.0.1.jar DominanceQueries.jar
