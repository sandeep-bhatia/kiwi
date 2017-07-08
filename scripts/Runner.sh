while [ true ]; 
do 
echo "running the passed in script now"
echo $1
chmod 777 *
sh ./$1
sleep $2;
echo "going to wait mode" 
done