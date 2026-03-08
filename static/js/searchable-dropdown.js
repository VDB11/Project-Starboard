/**
 * Reusable Searchable Dropdown Component
 * Uses Select2 with initial data loading AND backend fuzzy search
 */

class SearchableDropdown {
    constructor() {
        this.initSelect2Defaults();
    }

    initSelect2Defaults() {
        // Set default Select2 configuration
        $.fn.select2.defaults.set('theme', 'default');
        $.fn.select2.defaults.set('width', '100%');
    }

    /**
     * Make a dropdown searchable with fuzzy matching
     * @param {string} selector - jQuery selector for the dropdown
     * @param {object} options - Configuration options
     */
    makeSearchable(selector, options = {}) {
        const $element = $(selector);
        
        if ($element.length === 0) {
            console.warn(`Element not found: ${selector}`);
            return;
        }

        const config = {
            loadUrl: options.loadUrl || '',
            searchUrl: options.searchUrl || '',
            placeholder: options.placeholder || 'Select or search...',
            allowClear: options.allowClear !== false,
            additionalParams: options.additionalParams || {},
            ...options
        };

        // Destroy existing Select2 if present
        if ($element.hasClass('select2-hidden-accessible')) {
            $element.select2('destroy');
        }

        // Load initial data
        this.loadInitialData($element, config);

        // Store config for later use
        $element.data('searchable-config', config);

        return $element;
    }

    /**
     * Load initial dropdown data and setup Select2 with search
     */
    loadInitialData($element, config) {
        const self = this;
        
        // Build URL with params
        let url = config.loadUrl;
        const params = new URLSearchParams(config.additionalParams);
        if (params.toString()) {
            url += '?' + params.toString();
        }

        $.ajax({
            url: url,
            dataType: 'json',
            success: function(data) {
                // Clear existing options except the first (placeholder)
                $element.find('option:not(:first)').remove();
                
                // Sort data alphabetically by name/text
                data.sort((a, b) => {
                    let textA, textB;
                    
                    if (typeof a === 'string') {
                        textA = a;
                    } else if (a && typeof a === 'object') {
                        textA = a.name || a.text || a.id || a.value || '';
                    } else {
                        textA = String(a);
                    }
                    
                    if (typeof b === 'string') {
                        textB = b;
                    } else if (b && typeof b === 'object') {
                        textB = b.name || b.text || b.id || b.value || '';
                    } else {
                        textB = String(b);
                    }
                    
                    return textA.localeCompare(textB);
                });
                
                // Add ALL options to dropdown
                data.forEach(item => {
                    let text, value;
                    
                    if (typeof item === 'string') {
                        text = item;
                        value = item;
                    } else if (item && typeof item === 'object') {
                        text = item.name || item.text || item.id || item.value || JSON.stringify(item);
                        value = item.name || item.id || item.value || text;
                    } else {
                        text = String(item);
                        value = String(item);
                    }
                    
                    const option = new Option(text, value, false, false);
                    $element.append(option);
                });

                // Initialize Select2 with BACKEND SEARCH
                const select2Config = {
                    placeholder: config.placeholder,
                    allowClear: false,
                    width: '100%',
                    ajax: {
                        url: config.searchUrl,
                        dataType: 'json',
                        delay: 250,
                        data: function(params) {
                            return {
                                q: params.term || '',
                                ...config.additionalParams
                            };
                        },
                        processResults: function(data) {
                            if (!data || data.error) {
                                return { results: [] };
                            }

                            const results = data.map(item => ({
                                id: item.id || item.name || item,
                                text: item.name || item,
                                score: item.score
                            }));

                            return { results: results };
                        },
                        cache: true
                    },
                    minimumInputLength: 0
                };

                $element.select2(select2Config);
                
                // AUTO-FOCUS SEARCH BOX WHEN DROPDOWN OPENS
                $element.on('select2:open', function() {
                    setTimeout(function() {
                        const searchField = document.querySelector('.select2-search__field');
                        if (searchField) searchField.focus();
                    }, 0);
                });
            },
            error: function(err) {
                console.error('Error loading initial data:', err);
                $element.select2({
                    placeholder: config.placeholder,
                    allowClear: false,
                    width: '100%'
                });
            }
        });
    }

    /**
     * Reload dropdown data with new parameters
     */
    reload(selector, newParams = {}) {
        const $element = $(selector);
        const config = $element.data('searchable-config');
        
        if (config) {
            config.additionalParams = { ...config.additionalParams, ...newParams };
            $element.data('searchable-config', config);
            
            // Reload the data
            this.loadInitialData($element, config);
        }
    }

    /**
     * Update search parameters dynamically
     */
    updateSearchParams(selector, newParams) {
        const $element = $(selector);
        const config = $element.data('searchable-config');
        
        if (config) {
            config.additionalParams = { ...config.additionalParams, ...newParams };
            $element.data('searchable-config', config);
            
            // Reload with new params
            this.reload(selector, newParams);
        }
    }

    /**
     * Enable/disable dropdown
     */
    setEnabled(selector, enabled) {
        $(selector).prop('disabled', !enabled);
    }

    /**
     * Clear dropdown selection
     */
    clear(selector) {
        $(selector).val(null).trigger('change');
    }

    /**
     * Get selected value
     */
    getValue(selector) {
        return $(selector).val();
    }

    /**
     * Set value programmatically
     */
    setValue(selector, value, text) {
        const $element = $(selector);
        
        // Create option if it doesn't exist
        if ($element.find(`option[value="${value}"]`).length === 0) {
            const newOption = new Option(text || value, value, true, true);
            $element.append(newOption);
        } else {
            $element.val(value);
        }
        
        $element.trigger('change');
    }
}

// Create global instance
const searchableDropdown = new SearchableDropdown();

// Export for use in other scripts
window.SearchableDropdown = SearchableDropdown;
window.searchableDropdown = searchableDropdown;