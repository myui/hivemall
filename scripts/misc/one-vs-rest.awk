BEGIN{ FS="\t"; OFS="\t"; }
{
	possible_labels=$1;
	rowid=$2;
	label=$3;
	features=$4;

	label_count = split(possible_labels, label_array, ",");
 	
	for(i = 1; i <= label_count; i++) {
		if (label_array[i] == label)
			print rowid, label, 1, features;
		else
			print rowid, label_array[i], -1, features;
	}
}
END{}
