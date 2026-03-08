const STOP_COLORS = ["#00cc44", "#4facfe", "#ff9900", "#cc00ff", "#ff3300", "#00cccc", "#ffcc00"];

let stops = [
    { id: 1, label: "Origin",      wb: null, country: null, port: null },
    { id: 2, label: "Destination", wb: null, country: null, port: null }
];

let stopIdCounter = 3;

function getColor(index) {
    return STOP_COLORS[index % STOP_COLORS.length];
}

function relabelStops() {
    stops.forEach((stop, i) => {
        if (i === 0) stop.label = "Origin";
        else if (i === stops.length - 1) stop.label = "Destination";
        else stop.label = `Stop ${i}`;
    });
}

function buildStopHTML(stop, index) {
    const color  = getColor(index);
    const isLast = index === stops.length - 1;
    return `
    <div class="stop-card" id="stop-card-${stop.id}">
        <div class="stop-header" style="border-left: 4px solid ${color}; padding-left: 10px; margin-bottom: 12px;">
            <span class="stop-label">${stop.label}</span>
            ${stops.length > 2 ? `<button class="remove-stop-btn" onclick="removeStop(${stop.id})">✕</button>` : ''}
        </div>
        <div class="form-group">
            <label>Water Body</label>
            <div class="select-wrapper">
                <select id="wb-${stop.id}"><option value="">Select or search water body...</option></select>
            </div>
        </div>
        <div class="form-group">
            <label>Country</label>
            <div class="select-wrapper">
                <select id="country-${stop.id}" disabled><option value="">Select or search country...</option></select>
            </div>
        </div>
        <div class="form-group">
            <label>Port</label>
            <div class="select-wrapper">
                <select id="port-${stop.id}" disabled><option value="">Select or search port...</option></select>
            </div>
        </div>
        <button class="add-stop-btn" onclick="addStopAfter(${stop.id})">
            ${isLast ? '+ Add Destination' : '+ Add Stop Here'}
        </button>
    </div>`;
}

function loadWaterBodiesForStop(stopId, restoreValue) {
    const $el = $(`#wb-${stopId}`);

    $.ajax({
        url: '/api/water-bodies',
        dataType: 'json',
        success: function(data) {
            $el.find('option:not(:first)').remove();
            data.sort().forEach(wb => {
                $el.append(new Option(wb, wb, false, false));
            });

            $el.select2({
                placeholder: 'Select or search water body...',
                allowClear: false,
                width: '100%',
                ajax: {
                    url: '/api/search/water-bodies',
                    dataType: 'json',
                    delay: 250,
                    data: params => ({ q: params.term || '' }),
                    processResults: data => ({ results: data.map(i => ({ id: i.id || i, text: i.name || i })) }),
                    cache: true
                },
                minimumInputLength: 0
            });

            $el.on('select2:open', function() {
                setTimeout(() => { const f = document.querySelector('.select2-search__field'); if (f) f.focus(); }, 0);
            });

            if (restoreValue) {
                $el.val(restoreValue).trigger('change.select2');
            }
        }
    });
}

function loadCountriesForStop(stopId, waterBody, restoreValue) {
    const $el = $(`#country-${stopId}`);

    $.ajax({
        url: `/api/countries?water_body=${encodeURIComponent(waterBody)}`,
        dataType: 'json',
        success: function(data) {
            $el.find('option:not(:first)').remove();
            data.sort().forEach(c => {
                $el.append(new Option(c, c, false, false));
            });

            if ($el.hasClass('select2-hidden-accessible')) $el.select2('destroy');

            $el.select2({
                placeholder: 'Select or search country...',
                allowClear: false,
                width: '100%',
                ajax: {
                    url: '/api/search/countries',
                    dataType: 'json',
                    delay: 250,
                    data: params => ({ q: params.term || '', water_body: waterBody }),
                    processResults: data => ({ results: data.map(i => ({ id: i.id || i, text: i.name || i })) }),
                    cache: true
                },
                minimumInputLength: 0
            });

            $el.on('select2:open', function() {
                setTimeout(() => { const f = document.querySelector('.select2-search__field'); if (f) f.focus(); }, 0);
            });

            $el.prop('disabled', false);
            if (restoreValue) {
                $el.val(restoreValue).trigger('change.select2');
            }
        }
    });
}

function loadPortsForStop(stopId, waterBody, country, restoreValue) {
    const $el = $(`#port-${stopId}`);

    $.ajax({
        url: `/api/ports?water_body=${encodeURIComponent(waterBody)}&country_code=${encodeURIComponent(country)}`,
        dataType: 'json',
        success: function(data) {
            $el.find('option:not(:first)').remove();
            data.forEach(p => {
                $el.append(new Option(p.port_name, p.port_name, false, false));
            });

            if ($el.hasClass('select2-hidden-accessible')) $el.select2('destroy');

            $el.select2({
                placeholder: 'Select or search port...',
                allowClear: false,
                width: '100%',
                ajax: {
                    url: '/api/search/ports',
                    dataType: 'json',
                    delay: 250,
                    data: params => ({ q: params.term || '', water_body: waterBody, country_code: country }),
                    processResults: data => ({ results: data.map(i => ({ id: i.id || i, text: i.name || i })) }),
                    cache: true
                },
                minimumInputLength: 0
            });

            $el.on('select2:open', function() {
                setTimeout(() => { const f = document.querySelector('.select2-search__field'); if (f) f.focus(); }, 0);
            });

            $el.prop('disabled', false);
            if (restoreValue) {
                $el.val(restoreValue).trigger('change.select2');
            }
        }
    });
}

function attachEvents(stopId) {
    const stop = stops.find(s => s.id === stopId);

    $(`#wb-${stopId}`).on('change', function() {
        const waterBody = $(this).val();
        stop.wb = waterBody || null;
        stop.country = null;
        stop.port = null;

        const $country = $(`#country-${stopId}`);
        const $port    = $(`#port-${stopId}`);

        if ($country.hasClass('select2-hidden-accessible')) $country.select2('destroy');
        if ($port.hasClass('select2-hidden-accessible')) $port.select2('destroy');

        $country.empty().append('<option value="">Select or search country...</option>').prop('disabled', true);
        $port.empty().append('<option value="">Select or search port...</option>').prop('disabled', true);

        if (waterBody) loadCountriesForStop(stopId, waterBody, null);
        updateCalculateButton();
    });

    $(`#country-${stopId}`).on('change', function() {
        const country   = $(this).val();
        const waterBody = $(`#wb-${stopId}`).val();
        stop.country = country || null;
        stop.port    = null;

        const $port = $(`#port-${stopId}`);
        if ($port.hasClass('select2-hidden-accessible')) $port.select2('destroy');
        $port.empty().append('<option value="">Select or search port...</option>').prop('disabled', true);

        if (country && waterBody) loadPortsForStop(stopId, waterBody, country, null);
        updateCalculateButton();
    });

    $(`#port-${stopId}`).on('change', function() {
        stop.port = $(this).val() || null;
        updateCalculateButton();
    });
}

function initStopDropdowns(stop) {
    // Initialize country and port Select2 immediately (even disabled) so widget renders
    $(`#country-${stop.id}`).select2({
        placeholder: 'Select or search country...',
        allowClear: false,
        width: '100%',
        minimumInputLength: 0
    });

    $(`#port-${stop.id}`).select2({
        placeholder: 'Select or search port...',
        allowClear: false,
        width: '100%',
        minimumInputLength: 0
    });

    loadWaterBodiesForStop(stop.id, stop.wb);
    if (stop.wb) loadCountriesForStop(stop.id, stop.wb, stop.country);
    if (stop.wb && stop.country) loadPortsForStop(stop.id, stop.wb, stop.country, stop.port);
    attachEvents(stop.id);
}

function persistStopValues() {
    stops.forEach(stop => {
        stop.wb      = $(`#wb-${stop.id}`).val()      || null;
        stop.country = $(`#country-${stop.id}`).val() || null;
        stop.port    = $(`#port-${stop.id}`).val()    || null;
    });
}

function renderStops() {
    const container = document.getElementById('stops-container');
    container.innerHTML = stops.map((stop, i) => buildStopHTML(stop, i)).join('');
    stops.forEach(stop => initStopDropdowns(stop));
    updateCalculateButton();
}

function addStopAfter(stopId) {
    persistStopValues();
    const index   = stops.findIndex(s => s.id === stopId);
    const newStop = { id: stopIdCounter++, label: '', wb: null, country: null, port: null };
    stops.splice(index + 1, 0, newStop);
    relabelStops();

    // Update all labels without re-rendering
    stops.forEach((stop, i) => {
        const labelEl = document.querySelector(`#stop-card-${stop.id} .stop-label`);
        if (labelEl) labelEl.textContent = stop.label;
        // Update add-stop-btn text
        const addBtn = document.querySelector(`#stop-card-${stop.id} .add-stop-btn`);
        if (addBtn) addBtn.textContent = (i === stops.length - 1) ? '+ Add Destination' : '+ Add Stop Here';
        // Show/hide remove button based on stop count
        const removeBtn = document.querySelector(`#stop-card-${stop.id} .remove-stop-btn`);
        if (removeBtn) removeBtn.style.display = stops.length > 2 ? '' : 'none';
    });

    // Insert new card after the reference stop
    const refCard = document.getElementById(`stop-card-${stopId}`);
    const newHTML = buildStopHTML(newStop, index + 1);
    refCard.insertAdjacentHTML('afterend', newHTML);
    initStopDropdowns(newStop);

    // If only 2 stops before, now 3 — show remove buttons on all
    if (stops.length === 3) {
        stops.forEach(stop => {
            const removeBtn = document.querySelector(`#stop-card-${stop.id} .remove-stop-btn`);
            if (!removeBtn) {
                const header = document.querySelector(`#stop-card-${stop.id} .stop-header`);
                const btn = document.createElement('button');
                btn.className = 'remove-stop-btn';
                btn.textContent = '✕';
                btn.setAttribute('onclick', `removeStop(${stop.id})`);
                header.appendChild(btn);
            }
        });
    }

    updateCalculateButton();
}

function removeStop(stopId) {
    if (stops.length <= 2) return;
    persistStopValues();
    stops = stops.filter(s => s.id !== stopId);
    relabelStops();

    // Remove card from DOM smoothly
    const card = document.getElementById(`stop-card-${stopId}`);
    if (card) card.remove();

    // Update remaining labels and buttons
    stops.forEach((stop, i) => {
        const labelEl = document.querySelector(`#stop-card-${stop.id} .stop-label`);
        if (labelEl) labelEl.textContent = stop.label;
        const addBtn = document.querySelector(`#stop-card-${stop.id} .add-stop-btn`);
        if (addBtn) addBtn.textContent = (i === stops.length - 1) ? '+ Add Destination' : '+ Add Stop Here';
        // Hide remove buttons if back to 2 stops
        const removeBtn = document.querySelector(`#stop-card-${stop.id} .remove-stop-btn`);
        if (removeBtn && stops.length <= 2) removeBtn.remove();
    });

    updateCalculateButton();
}

function updateCalculateButton() {
    const allSelected = stops.every(stop => stop.port);
    document.getElementById('calculate-route').disabled = !allSelected;
}

function getStopsPayload() {
    persistStopValues();
    return stops.map(stop => ({
        port_name:    stop.port,
        country_code: stop.country,
        water_body:   stop.wb
    }));
}

$(document).ready(function() {
    renderStops();
    document.body.classList.add('select2-ready');
});

window.addStopAfter          = addStopAfter;
window.removeStop            = removeStop;
window.getStopsPayload       = getStopsPayload;
window.updateCalculateButton = updateCalculateButton;