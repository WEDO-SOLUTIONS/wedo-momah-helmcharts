const map_detections_clusters_colors = [
    '#46ff00',
    '#02750c',
    '#e400ff',
    '#fa67e6',
    '#ff0000',
    '#881b1b',
    '#ffff00',
];


function randomChoice(arr) {
    return arr[Math.floor(arr.length * Math.random())];
}

function randomClusterColor() {
    return randomChoice(map_detections_clusters_colors);
}