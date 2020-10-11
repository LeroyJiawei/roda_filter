package Structure;

public class StructureList {
	public double val;
	public int Id;
	//public int PivotId;
	public StructureList(int NumId, double value) {
		this.Id = NumId;
		this.val = value;
	}
	public StructureList(int NumId) {
		this.Id = NumId;
	}
	/*
	public List(int NumId, int pivotId, double value){
		this.PivotId = pivotId;
		this.Id = NumId;
		this.val = value;
	}
	*/
}
