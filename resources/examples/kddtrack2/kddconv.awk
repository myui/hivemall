BEGIN{ FS="\t" }
{
    rowid=$1;
    positive=$2;
    negative=$3;
    features=$4;
    for(i=5;i<=NF;i++)
    {
        features = features "," $i;
    }
    for(i=0;i<positive;i++)
    {
        print rowid "\t1.0\t" features
    }
    for(i=0;i<negative;i++)
    {
        print rowid "\t0.0\t" features
    }
}
END{}
