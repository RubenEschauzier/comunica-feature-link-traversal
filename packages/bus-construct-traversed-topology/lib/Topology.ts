export interface Topology{
    set: (node: string, parent: string, metadata: Record<string, any>) => void;

    getMetaData: (node: string) => Record<string, any> | undefined;

    setMetaData: (node: string, metadata: Record<string, any>) => void;

    // Function to set node to dereferenced. This is seperate because some algorithms might need to recalculate
    // priorities when this happens.
    setMetaDataDereferenced: (node: string, metadata: Record<string, any>) => void;

    getMetaDataAll: () => Record<string, any>[];

    getNodeToIndex: () => Record<string, number>;

    // Function to get the underlying graph datastructure, this can be from edge list, adjacency matrix, adjacency list
    getGraphDataStructure: () => any;
}