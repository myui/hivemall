BEGIN{ FS=" " }
{
    label=$1;
    features="{"
    for(i=2;i<=NF;i++)
    {
        if (i!=2)
            features = features ","
        feature = $i
        features = features "(" feature ")";
    }
    features = features "}"
    print NR "\t" label "\t" features;
}
END{}
