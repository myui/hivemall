BEGIN{ FS=" " }
{
    label=$1;
    features=$2;
    for(i=3;i<=NF;i++)
    {
        features = features "," $i;
    }
    print label ",[" features "]";
}
END{}