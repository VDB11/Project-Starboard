/**
 * ECA/MPA Handler Module
 * Handles rendering, toggling, and clearing of ECA/MPA zones on the map.
 * Depends on: map-initialization.js, Leaflet.js
 */

const ECA_MPA_STYLES = {
    ECA: {
        fillColor: '#FFD700',
        color: '#B8860B',
        fillOpacity: 0.25,
        weight: 2,
        opacity: 0.8
    },
    MPA: {
        fillColor: '#FF6B35',
        color: '#CC4400',
        fillOpacity: 0.25,
        weight: 2,
        opacity: 0.8
    }
};

window.ecaMpaLayer = null;

/**
 * Build a detail row for the popup.
 */
function _ecaRow(icon, label, value) {
    if (!value || value === '0' || value === '0.0') return '';
    return `
        <div style="display:grid; grid-template-columns:22px 1fr; gap:8px; align-items:start; margin-bottom:7px;">
            <span style="font-size:14px; line-height:1.4;">${icon}</span>
            <div>
                <div style="font-weight:600; color:#4a5568; font-size:11px; text-transform:uppercase; letter-spacing:0.4px;">${label}</div>
                <div style="color:#2d3748; font-size:13px; line-height:1.4;">${value}</div>
            </div>
        </div>`;
}

/**
 * Build popup HTML for ECA or MPA feature.
 */
function _buildEcaMpaPopup(props) {
    const isECA = props.type === 'ECA';
    const headerColor = isECA ? '#B8860B' : '#CC4400';
    const badgeColor  = isECA ? '#FFD700' : '#FF6B35';
    const badgeText   = isECA ? 'ECA' : 'MPA';
    const label       = isECA ? 'Emission Control Area' : 'Marine Protected Area';

    let rows = '';

    if (isECA) {
        rows += _ecaRow('📋', 'Regulation',  props.regulation);
    } else {
        rows += _ecaRow('🏛️', 'Designation', props.designation);
        rows += _ecaRow('🔰', 'IUCN Category', props.iucn_cat);
        rows += _ecaRow('📌', 'Status',      props.status);
        rows += _ecaRow('📅', 'Year Designated', props.status_yr && props.status_yr !== '0' ? props.status_yr : '');
        rows += _ecaRow('🌍', 'Country',     props.iso3);
        rows += _ecaRow('🏛️', 'Governance',  props.gov_type);
        rows += _ecaRow('📐', 'Marine Area', props.marine_area_km2 ? `${parseFloat(props.marine_area_km2).toLocaleString()} km²` : '');
        rows += _ecaRow('🚫', 'No-Take Zone', props.no_take === 'All' ? 'Yes — full no-take zone'
                                             : props.no_take === 'Part' ? 'Partial no-take zone'
                                             : props.no_take === 'None' ? 'No restrictions'
                                             : props.no_take);
    }

    return `
        <div style="font-family:'Roboto Slab',serif; min-width:230px; max-width:290px;">
            <div style="background:${headerColor}; padding:10px 14px; margin:-1px -1px 0 -1px; border-radius:6px 6px 0 0;">
                <div style="font-size:15px; font-weight:700; color:#fff; line-height:1.3;">${props.name}</div>
                <div style="font-size:12px; color:rgba(255,255,255,0.85); margin-top:3px;">${label}</div>
            </div>
            <div style="padding:12px 14px;">
                <div style="margin-bottom:10px;">
                    <span style="display:inline-block; padding:3px 10px; border-radius:12px;
                                background:${badgeColor}; color:#fff; font-size:11px;
                                font-weight:700; letter-spacing:0.5px;">${badgeText}</span>
                </div>
                ${rows || `<div style="font-size:12px; color:#888;">No additional data available.</div>`}
            </div>
        </div>`;
}

/**
 * Render ECA/MPA GeoJSON data onto the map.
 * @param {Object} geojsonData - GeoJSON FeatureCollection from the API
 */
function renderEcaMpaLayer(geojsonData) {
    clearEcaMpaLayer();

    if (!geojsonData || !geojsonData.features || geojsonData.features.length === 0) {
        return;
    }

    window.ecaMpaLayer = L.geoJSON(geojsonData, {
        style: function (feature) {
            const zoneType = feature.properties.type;
            return ECA_MPA_STYLES[zoneType] || ECA_MPA_STYLES.ECA;
        },
        onEachFeature: function (feature, layer) {
            const popupHtml = _buildEcaMpaPopup(feature.properties);
            layer.bindPopup(popupHtml, { maxWidth: 310 });

            layer.on('mouseover', function () {
                layer.setStyle({ fillOpacity: 0.45, weight: 3 });
            });
            layer.on('mouseout', function () {
                window.ecaMpaLayer && window.ecaMpaLayer.resetStyle(layer);
            });
        }
    }).addTo(map);
}

/**
 * Toggle ECA/MPA layer visibility.
 * @param {boolean} visible
 */
function toggleEcaMpaLayer(visible) {
    if (!window.ecaMpaLayer) return;
    if (visible) {
        if (!map.hasLayer(window.ecaMpaLayer)) window.ecaMpaLayer.addTo(map);
    } else {
        if (map.hasLayer(window.ecaMpaLayer)) map.removeLayer(window.ecaMpaLayer);
    }
}

/**
 * Clear ECA/MPA layer from map and reset reference.
 */
function clearEcaMpaLayer() {
    if (window.ecaMpaLayer) {
        if (map.hasLayer(window.ecaMpaLayer)) map.removeLayer(window.ecaMpaLayer);
        window.ecaMpaLayer = null;
    }
}

/**
 * Render ECA/MPA sidebar alert summary.
 * @param {Object} geojsonData - GeoJSON FeatureCollection
 */
function renderEcaMpaSidebar(geojsonData) {
    const container = document.getElementById('chokepoint-alerts');
    if (!container) return;

    const existing = document.getElementById('eca-mpa-alert');
    if (existing) existing.remove();

    if (!geojsonData || !geojsonData.features || geojsonData.features.length === 0) {
        return;
    }

    const ecaCount = geojsonData.features.filter(f => f.properties.type === 'ECA').length;
    const mpaCount = geojsonData.features.filter(f => f.properties.type === 'MPA').length;

    const parts = [];
    if (ecaCount > 0) parts.push(`<span style="color:#FFD700; font-weight:700;">${ecaCount} ECA</span>`);
    if (mpaCount > 0) parts.push(`<span style="color:#FF6B35; font-weight:700;">${mpaCount} MPA</span>`);

    const alert = document.createElement('div');
    alert.id = 'eca-mpa-alert';
    alert.style.cssText = `
        display:flex; align-items:center; gap:10px; padding:10px 12px;
        background:rgba(255,255,255,0.06); border-left:4px solid #FFD700;
        border-radius:6px; margin-top:10px;
    `;
    alert.innerHTML = `
        <span style="font-size:15px;">🌊</span>
        <div style="font-size:12px; font-weight:700; color:#fff;">
            Route crosses ${parts.join(' + ')} zone${(ecaCount + mpaCount) > 1 ? 's' : ''}
        </div>`;

    container.appendChild(alert);
}