const SEGMENT_COLORS = ["#0066ff", "#ff6600", "#00cc44", "#cc00ff", "#ff0033", "#00cccc", "#ffcc00"];

document.getElementById('calculate-route').addEventListener('click', async function() {
    const stops = getStopsPayload();

    document.getElementById('loading').style.display = 'block';
    document.getElementById('route-info').style.display = 'none';
    clearMap();

    try {
        const res = await fetch('/api/route', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ stops })
        });

        const data = await res.json();
        document.getElementById('loading').style.display = 'none';

        if (data.error) {
            alert('Error: ' + data.error);
            return;
        }

        const allCoords = [];

        data.segments.forEach((segment, i) => {
            const color = i === 0 ? '#0066ff' : SEGMENT_COLORS[i % SEGMENT_COLORS.length];

            const line = L.polyline(segment.coordinates, {
                color: color,
                weight: 4,
                opacity: 0.8,
                lineCap: 'round',
                lineJoin: 'round'
            }).addTo(map);

            line.bindTooltip(
                `<div style="font-family: 'Roboto Slab', serif; font-size: 13px; padding: 4px 2px;">
                    <strong style="font-size:14px; color:#111;">${segment.from.port_name}</strong>
                    <span style="color: #4facfe; margin: 0 6px; font-size:16px;">⟶</span>
                    <strong style="font-size:14px; color:#111;">${segment.to.port_name}</strong>
                    <div style="color: #333; font-size: 12px; font-weight:700; margin-top: 4px;">${segment.length.toLocaleString()} nautical miles</div>
                </div>`,
                { sticky: true, direction: 'top', opacity: 1.0 }
            );

            // Increase hit tolerance so hover triggers near the line
            line.on('mouseover', function(e) { line.openTooltip(e.latlng); });
            line.on('mouseout',  function()  { line.closeTooltip(); });

            window.routeLayers.push(line);
            allCoords.push(...segment.coordinates);

            if (i === 0) {
                addPortMarker(segment.from, 'green', 'Origin');
            }

            if (i === data.segments.length - 1) {
                addPortMarker(segment.to, 'red', 'Destination');
            } else {
                addPortMarker(segment.to, 'orange', `Stop ${i + 1}`);
            }
        });

        if (allCoords.length > 0) {
            map.fitBounds(L.latLngBounds(allCoords), { padding: [30, 30] });
        }

        document.getElementById('route-length').textContent =
            `Total Distance: ${data.total_length.toLocaleString()} nautical miles`;

        const segmentInfo = document.getElementById('segment-info');
        segmentInfo.innerHTML = data.segments.map((seg, i) =>
            `<div class="segment-item" style="border-left-color: ${SEGMENT_COLORS[i % SEGMENT_COLORS.length]}">
                <span><strong>${seg.from.port_name} → ${seg.to.port_name}</strong></span>
                <span>${seg.length.toLocaleString()} naut mi</span>
            </div>`
        ).join('');

        document.getElementById('route-info').style.display = 'block';

    } catch (err) {
        document.getElementById('loading').style.display = 'none';
        alert('Failed to calculate route: ' + err.message);
    }
});

function addPortMarker(port, color, label) {
    const iconUrl = `https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-${color}.png`;
    const icon = L.icon({
        iconUrl,
        shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
        iconSize: [25, 41],
        iconAnchor: [12, 41],
        popupAnchor: [1, -34],
        shadowSize: [41, 41]
    });

    const marker = L.marker([port.lat, port.lon], { icon })
        .addTo(map)
        .bindPopup(`<strong>${label}</strong><br>${port.port_name}<br><small>${port.port_code || ''}</small>`);

    window.portMarkers.push(marker);
}