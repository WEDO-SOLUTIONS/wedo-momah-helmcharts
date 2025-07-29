// from static/main.js from ds/wms project

class UrlControl {
    constructor(default_lat, default_lon, default_zoom) {
        const current_url = new URL(window.location.href)

        this.lat = current_url.searchParams.get('lat') || default_lat
        this.lon = current_url.searchParams.get('lon') || default_lon
        this.zoom = current_url.searchParams.get('zoom') || default_zoom
        this.base = current_url.searchParams.get('base') || default_base_layer

        this.timeout = null
    }

    remove_from_list(param, value) {
        if (this[param].includes(value)) {
            this[param] = this[param].filter(function(item) {
                return item !== value
            })
        }
        this._change_history()
    }

    add_to_list(param, value) {
        if (!this[param].includes(value)) {
            this[param].push(value)
        }
        this._change_history()
    }

    change_param(param, value, change_history = true) {
        this[param] = value
        if (change_history) this._change_history()
    }

    change_params(params) {
        for (const name in params) {
            this[name] = params[name]
        }
        this._change_history()
    }

    _change_history() {
        const current_url = new URL(window.location.href)

        for (const p of ['lat', 'lon', 'zoom', 'base']) {
            current_url.searchParams.set(p, this[p])
        }

        clearTimeout(this.timeout)
        this.timeout = setTimeout(function () {
            window.history.replaceState( {} , 'WMS UI', current_url.toString());
        }, 300)
    }
}
