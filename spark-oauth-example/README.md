This repo contains the spark java code to do wordcound and access the keyvault secret and write the output.
In hdi 4 we will get io.netty and other depencdency conflicts or method not found issues. 
because of the lower versions. 
to avoid that we will do shading of io.netty and run this. 

by default pom.xml doesnt contians shaded io.netty incase if we need to run the code in hdi 4 
user pom_hdi4.xml