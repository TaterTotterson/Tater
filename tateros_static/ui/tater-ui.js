//#region node_modules/@vue/shared/dist/shared.esm-bundler.js
// @__NO_SIDE_EFFECTS__
function e(e) {
	let t = /* @__PURE__ */ Object.create(null);
	for (let n of e.split(",")) t[n] = 1;
	return (e) => e in t;
}
var t = {}, n = [], r = () => {}, i = () => !1, a = (e) => e.charCodeAt(0) === 111 && e.charCodeAt(1) === 110 && (e.charCodeAt(2) > 122 || e.charCodeAt(2) < 97), o = (e) => e.startsWith("onUpdate:"), s = Object.assign, c = (e, t) => {
	let n = e.indexOf(t);
	n > -1 && e.splice(n, 1);
}, l = Object.prototype.hasOwnProperty, u = (e, t) => l.call(e, t), d = Array.isArray, f = (e) => S(e) === "[object Map]", p = (e) => S(e) === "[object Set]", m = (e) => S(e) === "[object Date]", h = (e) => S(e) === "[object RegExp]", g = (e) => typeof e == "function", _ = (e) => typeof e == "string", v = (e) => typeof e == "symbol", y = (e) => typeof e == "object" && !!e, b = (e) => (y(e) || g(e)) && g(e.then) && g(e.catch), x = Object.prototype.toString, S = (e) => x.call(e), C = (e) => S(e).slice(8, -1), w = (e) => S(e) === "[object Object]", T = (e) => _(e) && e !== "NaN" && e[0] !== "-" && "" + parseInt(e, 10) === e, E = /* @__PURE__ */ e(",key,ref,ref_for,ref_key,onVnodeBeforeMount,onVnodeMounted,onVnodeBeforeUpdate,onVnodeUpdated,onVnodeBeforeUnmount,onVnodeUnmounted"), D = (e) => {
	let t = /* @__PURE__ */ Object.create(null);
	return ((n) => t[n] || (t[n] = e(n)));
}, O = /-\w/g, k = D((e) => e.replace(O, (e) => e.slice(1).toUpperCase())), A = /\B([A-Z])/g, j = D((e) => e.replace(A, "-$1").toLowerCase()), M = D((e) => e.charAt(0).toUpperCase() + e.slice(1)), N = D((e) => e ? `on${M(e)}` : ""), P = (e, t) => !Object.is(e, t), F = (e, ...t) => {
	for (let n = 0; n < e.length; n++) e[n](...t);
}, I = (e, t, n, r = !1) => {
	Object.defineProperty(e, t, {
		configurable: !0,
		enumerable: !1,
		writable: r,
		value: n
	});
}, ee = (e) => {
	let t = parseFloat(e);
	return isNaN(t) ? e : t;
}, te = (e) => {
	let t = _(e) ? Number(e) : NaN;
	return isNaN(t) ? e : t;
}, ne, L = () => ne ||= typeof globalThis < "u" ? globalThis : typeof self < "u" ? self : typeof window < "u" ? window : typeof global < "u" ? global : {};
function R(e) {
	if (d(e)) {
		let t = {};
		for (let n = 0; n < e.length; n++) {
			let r = e[n], i = _(r) ? ae(r) : R(r);
			if (i) for (let e in i) t[e] = i[e];
		}
		return t;
	}
	if (_(e) || y(e)) return e;
}
var z = /;(?![^(]*\))/g, re = /:([^]+)/, ie = /\/\*[^]*?\*\//g;
function ae(e) {
	let t = {};
	return e.replace(ie, "").split(z).forEach((e) => {
		if (e) {
			let n = e.split(re);
			n.length > 1 && (t[n[0].trim()] = n[1].trim());
		}
	}), t;
}
function B(e) {
	let t = "";
	if (_(e)) t = e;
	else if (d(e)) for (let n = 0; n < e.length; n++) {
		let r = B(e[n]);
		r && (t += r + " ");
	}
	else if (y(e)) for (let n in e) e[n] && (t += n + " ");
	return t.trim();
}
var oe = "itemscope,allowfullscreen,formnovalidate,ismap,nomodule,novalidate,readonly", se = /* @__PURE__ */ e(oe);
oe + "";
function ce(e) {
	return !!e || e === "";
}
function le(e, t) {
	if (e.length !== t.length) return !1;
	let n = !0;
	for (let r = 0; n && r < e.length; r++) n = ue(e[r], t[r]);
	return n;
}
function ue(e, t) {
	if (e === t) return !0;
	let n = m(e), r = m(t);
	if (n || r) return n && r ? e.getTime() === t.getTime() : !1;
	if (n = v(e), r = v(t), n || r) return e === t;
	if (n = d(e), r = d(t), n || r) return n && r ? le(e, t) : !1;
	if (n = y(e), r = y(t), n || r) {
		if (!n || !r || Object.keys(e).length !== Object.keys(t).length) return !1;
		for (let n in e) {
			let r = e.hasOwnProperty(n), i = t.hasOwnProperty(n);
			if (r && !i || !r && i || !ue(e[n], t[n])) return !1;
		}
	}
	return String(e) === String(t);
}
function de(e, t) {
	return e.findIndex((e) => ue(e, t));
}
var fe = (e) => !!(e && e.__v_isRef === !0), V = (e) => _(e) ? e : e == null ? "" : d(e) || y(e) && (e.toString === x || !g(e.toString)) ? fe(e) ? V(e.value) : JSON.stringify(e, pe, 2) : String(e), pe = (e, t) => fe(t) ? pe(e, t.value) : f(t) ? { [`Map(${t.size})`]: [...t.entries()].reduce((e, [t, n], r) => (e[me(t, r) + " =>"] = n, e), {}) } : p(t) ? { [`Set(${t.size})`]: [...t.values()].map((e) => me(e)) } : v(t) ? me(t) : y(t) && !d(t) && !w(t) ? String(t) : t, me = (e, t = "") => v(e) ? `Symbol(${e.description ?? t})` : e, he, ge = class {
	constructor(e = !1) {
		this.detached = e, this._active = !0, this._on = 0, this.effects = [], this.cleanups = [], this._isPaused = !1, this._warnOnRun = !0, this.__v_skip = !0, !e && he && (he.active ? (this.parent = he, this.index = (he.scopes || (he.scopes = [])).push(this) - 1) : (this._active = !1, this._warnOnRun = !1));
	}
	get active() {
		return this._active;
	}
	pause() {
		if (this._active) {
			this._isPaused = !0;
			let e, t;
			if (this.scopes) {
				let n = this.scopes.slice();
				for (e = 0, t = n.length; e < t; e++) n[e].pause();
			}
			for (e = 0, t = this.effects.length; e < t; e++) this.effects[e].pause();
		}
	}
	resume() {
		if (this._active && this._isPaused) {
			this._isPaused = !1;
			let e, t;
			if (this.scopes) {
				let n = this.scopes.slice();
				for (e = 0, t = n.length; e < t; e++) n[e].resume();
			}
			let n = this.effects.slice();
			for (e = 0, t = n.length; e < t; e++) n[e].resume();
		}
	}
	run(e) {
		if (this._active) {
			let t = he;
			try {
				return he = this, e();
			} finally {
				he = t;
			}
		}
	}
	on() {
		++this._on === 1 && (this.prevScope = he, he = this);
	}
	off() {
		if (this._on > 0 && --this._on === 0) {
			if (he === this) he = this.prevScope;
			else {
				let e = he;
				for (; e;) {
					if (e.prevScope === this) {
						e.prevScope = this.prevScope;
						break;
					}
					e = e.prevScope;
				}
			}
			this.prevScope = void 0;
		}
	}
	stop(e) {
		if (this._active) {
			this._active = !1;
			let t, n;
			for (t = 0, n = this.effects.length; t < n; t++) this.effects[t].stop();
			for (this.effects.length = 0, t = 0, n = this.cleanups.length; t < n; t++) this.cleanups[t]();
			if (this.cleanups.length = 0, this.scopes) {
				let e = this.scopes.slice();
				for (t = 0, n = e.length; t < n; t++) e[t].stop(!0);
				this.scopes.length = 0;
			}
			if (!this.detached && this.parent && !e) {
				let e = this.parent.scopes.pop();
				e && e !== this && (this.parent.scopes[this.index] = e, e.index = this.index);
			}
			this.parent = void 0;
		}
	}
};
function _e() {
	return he;
}
var ve, ye = /* @__PURE__ */ new WeakSet(), be = class {
	constructor(e) {
		this.fn = e, this.deps = void 0, this.depsTail = void 0, this.flags = 5, this.next = void 0, this.cleanup = void 0, this.scheduler = void 0, he && (he.active ? he.effects.push(this) : this.flags &= -2);
	}
	pause() {
		this.flags |= 64;
	}
	resume() {
		this.flags & 64 && (this.flags &= -65, ye.has(this) && (ye.delete(this), this.trigger()));
	}
	notify() {
		this.flags & 2 && !(this.flags & 32) || this.flags & 8 || Ce(this);
	}
	run() {
		if (!(this.flags & 1)) return this.fn();
		this.flags |= 2, Ie(this), Ee(this);
		let e = ve, t = Me;
		ve = this, Me = !0;
		try {
			return this.fn();
		} finally {
			De(this), ve = e, Me = t, this.flags &= -3;
		}
	}
	stop() {
		if (this.flags & 1) {
			for (let e = this.deps; e; e = e.nextDep) Ae(e);
			this.deps = this.depsTail = void 0, Ie(this), this.onStop && this.onStop(), this.flags &= -2;
		}
	}
	trigger() {
		this.flags & 64 ? ye.add(this) : this.scheduler ? this.scheduler() : this.runIfDirty();
	}
	runIfDirty() {
		Oe(this) && this.run();
	}
	get dirty() {
		return Oe(this);
	}
}, xe = 0, H, Se;
function Ce(e, t = !1) {
	if (e.flags |= 8, t) {
		e.next = Se, Se = e;
		return;
	}
	e.next = H, H = e;
}
function we() {
	xe++;
}
function Te() {
	if (--xe > 0) return;
	if (Se) {
		let e = Se;
		for (Se = void 0; e;) {
			let t = e.next;
			e.next = void 0, e.flags &= -9, e = t;
		}
	}
	let e;
	for (; H;) {
		let t = H;
		for (H = void 0; t;) {
			let n = t.next;
			if (t.next = void 0, t.flags &= -9, t.flags & 1) try {
				t.trigger();
			} catch (t) {
				e ||= t;
			}
			t = n;
		}
	}
	if (e) throw e;
}
function Ee(e) {
	for (let t = e.deps; t; t = t.nextDep) t.version = -1, t.prevActiveLink = t.dep.activeLink, t.dep.activeLink = t;
}
function De(e) {
	let t, n = e.depsTail, r = n;
	for (; r;) {
		let e = r.prevDep;
		r.version === -1 ? (r === n && (n = e), Ae(r), je(r)) : t = r, r.dep.activeLink = r.prevActiveLink, r.prevActiveLink = void 0, r = e;
	}
	e.deps = t, e.depsTail = n;
}
function Oe(e) {
	for (let t = e.deps; t; t = t.nextDep) if (t.dep.version !== t.version || t.dep.computed && (ke(t.dep.computed) || t.dep.version !== t.version)) return !0;
	return !!e._dirty;
}
function ke(e) {
	if (e.flags & 4 && !(e.flags & 16) || (e.flags &= -17, e.globalVersion === Le) || (e.globalVersion = Le, !e.isSSR && e.flags & 128 && (!e.deps && !e._dirty || !Oe(e)))) return;
	e.flags |= 2;
	let t = e.dep, n = ve, r = Me;
	ve = e, Me = !0;
	try {
		Ee(e);
		let n = e.fn(e._value);
		(t.version === 0 || P(n, e._value)) && (e.flags |= 128, e._value = n, t.version++);
	} catch (e) {
		throw t.version++, e;
	} finally {
		ve = n, Me = r, De(e), e.flags &= -3;
	}
}
function Ae(e, t = !1) {
	let { dep: n, prevSub: r, nextSub: i } = e;
	if (r && (r.nextSub = i, e.prevSub = void 0), i && (i.prevSub = r, e.nextSub = void 0), n.subs === e && (n.subs = r, !r && n.computed)) {
		n.computed.flags &= -5;
		for (let e = n.computed.deps; e; e = e.nextDep) Ae(e, !0);
	}
	!t && !--n.sc && n.map && n.map.delete(n.key);
}
function je(e) {
	let { prevDep: t, nextDep: n } = e;
	t && (t.nextDep = n, e.prevDep = void 0), n && (n.prevDep = t, e.nextDep = void 0);
}
var Me = !0, Ne = [];
function Pe() {
	Ne.push(Me), Me = !1;
}
function Fe() {
	let e = Ne.pop();
	Me = e === void 0 || e;
}
function Ie(e) {
	let { cleanup: t } = e;
	if (e.cleanup = void 0, t) {
		let e = ve;
		ve = void 0;
		try {
			t();
		} finally {
			ve = e;
		}
	}
}
var Le = 0, Re = class {
	constructor(e, t) {
		this.sub = e, this.dep = t, this.version = t.version, this.nextDep = this.prevDep = this.nextSub = this.prevSub = this.prevActiveLink = void 0;
	}
}, ze = class {
	constructor(e) {
		this.computed = e, this.version = 0, this.activeLink = void 0, this.subs = void 0, this.map = void 0, this.key = void 0, this.sc = 0, this.__v_skip = !0;
	}
	track(e) {
		if (!ve || !Me || ve === this.computed) return;
		let t = this.activeLink;
		if (t === void 0 || t.sub !== ve) t = this.activeLink = new Re(ve, this), ve.deps ? (t.prevDep = ve.depsTail, ve.depsTail.nextDep = t, ve.depsTail = t) : ve.deps = ve.depsTail = t, Be(t);
		else if (t.version === -1 && (t.version = this.version, t.nextDep)) {
			let e = t.nextDep;
			e.prevDep = t.prevDep, t.prevDep && (t.prevDep.nextDep = e), t.prevDep = ve.depsTail, t.nextDep = void 0, ve.depsTail.nextDep = t, ve.depsTail = t, ve.deps === t && (ve.deps = e);
		}
		return t;
	}
	trigger(e) {
		this.version++, Le++, this.notify(e);
	}
	notify(e) {
		we();
		try {
			for (let e = this.subs; e; e = e.prevSub) e.sub.notify() && e.sub.dep.notify();
		} finally {
			Te();
		}
	}
};
function Be(e) {
	if (e.dep.sc++, e.sub.flags & 4) {
		let t = e.dep.computed;
		if (t && !e.dep.subs) {
			t.flags |= 20;
			for (let e = t.deps; e; e = e.nextDep) Be(e);
		}
		let n = e.dep.subs;
		n !== e && (e.prevSub = n, n && (n.nextSub = e)), e.dep.subs = e;
	}
}
var Ve = /* @__PURE__ */ new WeakMap(), He = /* @__PURE__ */ Symbol(""), Ue = /* @__PURE__ */ Symbol(""), We = /* @__PURE__ */ Symbol("");
function Ge(e, t, n) {
	if (Me && ve) {
		let t = Ve.get(e);
		t || Ve.set(e, t = /* @__PURE__ */ new Map());
		let r = t.get(n);
		r || (t.set(n, r = new ze()), r.map = t, r.key = n), r.track();
	}
}
function Ke(e, t, n, r, i, a) {
	let o = Ve.get(e);
	if (!o) {
		Le++;
		return;
	}
	let s = (e) => {
		e && e.trigger();
	};
	if (we(), t === "clear") o.forEach(s);
	else {
		let i = d(e), a = i && T(n);
		if (i && n === "length") {
			let e = Number(r);
			o.forEach((t, n) => {
				(n === "length" || n === We || !v(n) && n >= e) && s(t);
			});
		} else switch ((n !== void 0 || o.has(void 0)) && s(o.get(n)), a && s(o.get(We)), t) {
			case "add":
				i ? a && s(o.get("length")) : (s(o.get(He)), f(e) && s(o.get(Ue)));
				break;
			case "delete":
				i || (s(o.get(He)), f(e) && s(o.get(Ue)));
				break;
			case "set": f(e) && s(o.get(He));
		}
	}
	Te();
}
function qe(e) {
	let t = /* @__PURE__ */ Pt(e);
	return t === e ? t : (Ge(t, "iterate", We), /* @__PURE__ */ Mt(e) ? t : t.map(It));
}
function Je(e) {
	return Ge(e = /* @__PURE__ */ Pt(e), "iterate", We), e;
}
function Ye(e, t) {
	return /* @__PURE__ */ jt(e) ? Lt(/* @__PURE__ */ At(e) ? It(t) : t) : It(t);
}
var Xe = {
	__proto__: null,
	[Symbol.iterator]() {
		return Ze(this, Symbol.iterator, (e) => Ye(this, e));
	},
	concat(...e) {
		return qe(this).concat(...e.map((e) => d(e) ? qe(e) : e));
	},
	entries() {
		return Ze(this, "entries", (e) => (e[1] = Ye(this, e[1]), e));
	},
	every(e, t) {
		return $e(this, "every", e, t, void 0, arguments);
	},
	filter(e, t) {
		return $e(this, "filter", e, t, (e) => e.map((e) => Ye(this, e)), arguments);
	},
	find(e, t) {
		return $e(this, "find", e, t, (e) => Ye(this, e), arguments);
	},
	findIndex(e, t) {
		return $e(this, "findIndex", e, t, void 0, arguments);
	},
	findLast(e, t) {
		return $e(this, "findLast", e, t, (e) => Ye(this, e), arguments);
	},
	findLastIndex(e, t) {
		return $e(this, "findLastIndex", e, t, void 0, arguments);
	},
	forEach(e, t) {
		return $e(this, "forEach", e, t, void 0, arguments);
	},
	includes(...e) {
		return tt(this, "includes", e);
	},
	indexOf(...e) {
		return tt(this, "indexOf", e);
	},
	join(e) {
		return qe(this).join(e);
	},
	lastIndexOf(...e) {
		return tt(this, "lastIndexOf", e);
	},
	map(e, t) {
		return $e(this, "map", e, t, void 0, arguments);
	},
	pop() {
		return nt(this, "pop");
	},
	push(...e) {
		return nt(this, "push", e);
	},
	reduce(e, ...t) {
		return et(this, "reduce", e, t);
	},
	reduceRight(e, ...t) {
		return et(this, "reduceRight", e, t);
	},
	shift() {
		return nt(this, "shift");
	},
	some(e, t) {
		return $e(this, "some", e, t, void 0, arguments);
	},
	splice(...e) {
		return nt(this, "splice", e);
	},
	toReversed() {
		return qe(this).toReversed();
	},
	toSorted(e) {
		return qe(this).toSorted(e);
	},
	toSpliced(...e) {
		return qe(this).toSpliced(...e);
	},
	unshift(...e) {
		return nt(this, "unshift", e);
	},
	values() {
		return Ze(this, "values", (e) => Ye(this, e));
	}
};
function Ze(e, t, n) {
	let r = Je(e), i = r[t]();
	return r !== e && !/* @__PURE__ */ Mt(e) && (i._next = i.next, i.next = () => {
		let e = i._next();
		return e.done || (e.value = n(e.value)), e;
	}), i;
}
var Qe = Array.prototype;
function $e(e, t, n, r, i, a) {
	let o = Je(e), s = o !== e && !/* @__PURE__ */ Mt(e), c = o[t];
	if (c !== Qe[t]) {
		let t = c.apply(e, a);
		return s ? It(t) : t;
	}
	let l = n;
	o !== e && (s ? l = function(t, r) {
		return n.call(this, Ye(e, t), r, e);
	} : n.length > 2 && (l = function(t, r) {
		return n.call(this, t, r, e);
	}));
	let u = c.call(o, l, r);
	return s && i ? i(u) : u;
}
function et(e, t, n, r) {
	let i = Je(e), a = i !== e && !/* @__PURE__ */ Mt(e), o = n, s = !1;
	i !== e && (a ? (s = r.length === 0, o = function(t, r, i) {
		return s && (s = !1, t = Ye(e, t)), n.call(this, t, Ye(e, r), i, e);
	}) : n.length > 3 && (o = function(t, r, i) {
		return n.call(this, t, r, i, e);
	}));
	let c = i[t](o, ...r);
	return s ? Ye(e, c) : c;
}
function tt(e, t, n) {
	let r = /* @__PURE__ */ Pt(e);
	Ge(r, "iterate", We);
	let i = r[t](...n);
	return (i === -1 || i === !1) && /* @__PURE__ */ Nt(n[0]) ? (n[0] = /* @__PURE__ */ Pt(n[0]), r[t](...n)) : i;
}
function nt(e, t, n = []) {
	Pe(), we();
	let r = (/* @__PURE__ */ Pt(e))[t].apply(e, n);
	return Te(), Fe(), r;
}
var rt = /* @__PURE__ */ e("__proto__,__v_isRef,__isVue"), it = new Set(/* @__PURE__ */ Object.getOwnPropertyNames(Symbol).filter((e) => e !== "arguments" && e !== "caller").map((e) => Symbol[e]).filter(v));
function at(e) {
	v(e) || (e = String(e));
	let t = /* @__PURE__ */ Pt(this);
	return Ge(t, "has", e), t.hasOwnProperty(e);
}
var ot = class {
	constructor(e = !1, t = !1) {
		this._isReadonly = e, this._isShallow = t;
	}
	get(e, t, n) {
		if (t === "__v_skip") return e.__v_skip;
		let r = this._isReadonly, i = this._isShallow;
		if (t === "__v_isReactive") return !r;
		if (t === "__v_isReadonly") return r;
		if (t === "__v_isShallow") return i;
		if (t === "__v_raw") return n === (r ? i ? wt : Ct : i ? St : xt).get(e) || Object.getPrototypeOf(e) === Object.getPrototypeOf(n) ? e : void 0;
		let a = d(e);
		if (!r) {
			let e;
			if (a && (e = Xe[t])) return e;
			if (t === "hasOwnProperty") return at;
		}
		let o = Reflect.get(e, t, /* @__PURE__ */ Rt(e) ? e : n);
		if ((v(t) ? it.has(t) : rt(t)) || (r || Ge(e, "get", t), i)) return o;
		if (/* @__PURE__ */ Rt(o)) {
			let e = a && T(t) ? o : o.value;
			return r && y(e) ? /* @__PURE__ */ Ot(e) : e;
		}
		return y(o) ? r ? /* @__PURE__ */ Ot(o) : /* @__PURE__ */ Et(o) : o;
	}
}, st = class extends ot {
	constructor(e = !1) {
		super(!1, e);
	}
	set(e, t, n, r) {
		let i = e[t], a = d(e) && T(t);
		if (!this._isShallow) {
			let e = /* @__PURE__ */ jt(i);
			if (!/* @__PURE__ */ Mt(n) && !/* @__PURE__ */ jt(n) && (i = /* @__PURE__ */ Pt(i), n = /* @__PURE__ */ Pt(n)), !a && /* @__PURE__ */ Rt(i) && !/* @__PURE__ */ Rt(n)) return e || (i.value = n), !0;
		}
		let o = a ? Number(t) < e.length : u(e, t), s = Reflect.set(e, t, n, /* @__PURE__ */ Rt(e) ? e : r);
		return e === /* @__PURE__ */ Pt(r) && s && (o ? P(n, i) && Ke(e, "set", t, n, i) : Ke(e, "add", t, n)), s;
	}
	deleteProperty(e, t) {
		let n = u(e, t), r = e[t], i = Reflect.deleteProperty(e, t);
		return i && n && Ke(e, "delete", t, void 0, r), i;
	}
	has(e, t) {
		let n = Reflect.has(e, t);
		return (!v(t) || !it.has(t)) && Ge(e, "has", t), n;
	}
	ownKeys(e) {
		return Ge(e, "iterate", d(e) ? "length" : He), Reflect.ownKeys(e);
	}
}, ct = class extends ot {
	constructor(e = !1) {
		super(!0, e);
	}
	set(e, t) {
		return !0;
	}
	deleteProperty(e, t) {
		return !0;
	}
}, lt = /* @__PURE__ */ new st(), ut = /* @__PURE__ */ new ct(), dt = /* @__PURE__ */ new st(!0), ft = (e) => e, pt = (e) => Reflect.getPrototypeOf(e);
function mt(e, t, n) {
	return function(...r) {
		let i = this.__v_raw, a = /* @__PURE__ */ Pt(i), o = f(a), c = e === "entries" || e === Symbol.iterator && o, l = e === "keys" && o, u = i[e](...r), d = n ? ft : t ? Lt : It;
		return !t && Ge(a, "iterate", l ? Ue : He), s(Object.create(u), { next() {
			let { value: e, done: t } = u.next();
			return t ? {
				value: e,
				done: t
			} : {
				value: c ? [d(e[0]), d(e[1])] : d(e),
				done: t
			};
		} });
	};
}
function ht(e) {
	return function(...t) {
		return e === "delete" ? !1 : e === "clear" ? void 0 : this;
	};
}
function gt(e, t) {
	let n = {
		get(n) {
			let r = this.__v_raw, i = /* @__PURE__ */ Pt(r), a = /* @__PURE__ */ Pt(n);
			e || (P(n, a) && Ge(i, "get", n), Ge(i, "get", a));
			let { has: o } = pt(i), s = t ? ft : e ? Lt : It;
			if (o.call(i, n)) return s(r.get(n));
			if (o.call(i, a)) return s(r.get(a));
			r !== i && r.get(n);
		},
		get size() {
			let t = this.__v_raw;
			return !e && Ge(/* @__PURE__ */ Pt(t), "iterate", He), t.size;
		},
		has(t) {
			let n = this.__v_raw, r = /* @__PURE__ */ Pt(n), i = /* @__PURE__ */ Pt(t);
			return e || (P(t, i) && Ge(r, "has", t), Ge(r, "has", i)), t === i ? n.has(t) : n.has(t) || n.has(i);
		},
		forEach(n, r) {
			let i = this, a = i.__v_raw, o = /* @__PURE__ */ Pt(a), s = t ? ft : e ? Lt : It;
			return !e && Ge(o, "iterate", He), a.forEach((e, t) => n.call(r, s(e), s(t), i));
		}
	};
	return s(n, e ? {
		add: ht("add"),
		set: ht("set"),
		delete: ht("delete"),
		clear: ht("clear")
	} : {
		add(e) {
			let n = /* @__PURE__ */ Pt(this), r = pt(n), i = /* @__PURE__ */ Pt(e), a = !t && !/* @__PURE__ */ Mt(e) && !/* @__PURE__ */ jt(e) ? i : e;
			return r.has.call(n, a) || P(e, a) && r.has.call(n, e) || P(i, a) && r.has.call(n, i) || (n.add(a), Ke(n, "add", a, a)), this;
		},
		set(e, n) {
			!t && !/* @__PURE__ */ Mt(n) && !/* @__PURE__ */ jt(n) && (n = /* @__PURE__ */ Pt(n));
			let r = /* @__PURE__ */ Pt(this), { has: i, get: a } = pt(r), o = i.call(r, e);
			o ||= (e = /* @__PURE__ */ Pt(e), i.call(r, e));
			let s = a.call(r, e);
			return r.set(e, n), o ? P(n, s) && Ke(r, "set", e, n, s) : Ke(r, "add", e, n), this;
		},
		delete(e) {
			let t = /* @__PURE__ */ Pt(this), { has: n, get: r } = pt(t), i = n.call(t, e);
			i ||= (e = /* @__PURE__ */ Pt(e), n.call(t, e));
			let a = r ? r.call(t, e) : void 0, o = t.delete(e);
			return i && Ke(t, "delete", e, void 0, a), o;
		},
		clear() {
			let e = /* @__PURE__ */ Pt(this), t = e.size !== 0, n = e.clear();
			return t && Ke(e, "clear", void 0, void 0, void 0), n;
		}
	}), [
		"keys",
		"values",
		"entries",
		Symbol.iterator
	].forEach((r) => {
		n[r] = mt(r, e, t);
	}), n;
}
function _t(e, t) {
	let n = gt(e, t);
	return (t, r, i) => r === "__v_isReactive" ? !e : r === "__v_isReadonly" ? e : r === "__v_raw" ? t : Reflect.get(u(n, r) && r in t ? n : t, r, i);
}
var vt = { get: /* @__PURE__ */ _t(!1, !1) }, yt = { get: /* @__PURE__ */ _t(!1, !0) }, bt = { get: /* @__PURE__ */ _t(!0, !1) }, xt = /* @__PURE__ */ new WeakMap(), St = /* @__PURE__ */ new WeakMap(), Ct = /* @__PURE__ */ new WeakMap(), wt = /* @__PURE__ */ new WeakMap();
function Tt(e) {
	switch (e) {
		case "Object":
		case "Array": return 1;
		case "Map":
		case "Set":
		case "WeakMap":
		case "WeakSet": return 2;
		default: return 0;
	}
}
// @__NO_SIDE_EFFECTS__
function Et(e) {
	return /* @__PURE__ */ jt(e) ? e : kt(e, !1, lt, vt, xt);
}
// @__NO_SIDE_EFFECTS__
function Dt(e) {
	return kt(e, !1, dt, yt, St);
}
// @__NO_SIDE_EFFECTS__
function Ot(e) {
	return kt(e, !0, ut, bt, Ct);
}
function kt(e, t, n, r, i) {
	if (!y(e) || e.__v_raw && !(t && e.__v_isReactive) || e.__v_skip || !Object.isExtensible(e)) return e;
	let a = i.get(e);
	if (a) return a;
	let o = Tt(C(e));
	if (o === 0) return e;
	let s = new Proxy(e, o === 2 ? r : n);
	return i.set(e, s), s;
}
// @__NO_SIDE_EFFECTS__
function At(e) {
	return /* @__PURE__ */ jt(e) ? /* @__PURE__ */ At(e.__v_raw) : !!(e && e.__v_isReactive);
}
// @__NO_SIDE_EFFECTS__
function jt(e) {
	return !!(e && e.__v_isReadonly);
}
// @__NO_SIDE_EFFECTS__
function Mt(e) {
	return !!(e && e.__v_isShallow);
}
// @__NO_SIDE_EFFECTS__
function Nt(e) {
	return e ? !!e.__v_raw : !1;
}
// @__NO_SIDE_EFFECTS__
function Pt(e) {
	let t = e && e.__v_raw;
	return t ? /* @__PURE__ */ Pt(t) : e;
}
function Ft(e) {
	return !u(e, "__v_skip") && Object.isExtensible(e) && I(e, "__v_skip", !0), e;
}
var It = (e) => y(e) ? /* @__PURE__ */ Et(e) : e, Lt = (e) => y(e) ? /* @__PURE__ */ Ot(e) : e;
// @__NO_SIDE_EFFECTS__
function Rt(e) {
	return e ? e.__v_isRef === !0 : !1;
}
// @__NO_SIDE_EFFECTS__
function U(e) {
	return zt(e, !1);
}
function zt(e, t) {
	return /* @__PURE__ */ Rt(e) ? e : new Bt(e, t);
}
var Bt = class {
	constructor(e, t) {
		this.dep = new ze(), this.__v_isRef = !0, this.__v_isShallow = !1, this._rawValue = t ? e : /* @__PURE__ */ Pt(e), this._value = t ? e : It(e), this.__v_isShallow = t;
	}
	get value() {
		return this.dep.track(), this._value;
	}
	set value(e) {
		let t = this._rawValue, n = this.__v_isShallow || /* @__PURE__ */ Mt(e) || /* @__PURE__ */ jt(e);
		e = n ? e : /* @__PURE__ */ Pt(e), P(e, t) && (this._rawValue = e, this._value = n ? e : It(e), this.dep.trigger());
	}
};
function Vt(e) {
	return /* @__PURE__ */ Rt(e) ? e.value : e;
}
var Ht = {
	get: (e, t, n) => t === "__v_raw" ? e : Vt(Reflect.get(e, t, n)),
	set: (e, t, n, r) => {
		let i = e[t];
		return /* @__PURE__ */ Rt(i) && !/* @__PURE__ */ Rt(n) ? (i.value = n, !0) : Reflect.set(e, t, n, r);
	}
};
function Ut(e) {
	return /* @__PURE__ */ At(e) ? e : new Proxy(e, Ht);
}
var Wt = class {
	constructor(e, t, n) {
		this.fn = e, this.setter = t, this._value = void 0, this.dep = new ze(this), this.__v_isRef = !0, this.deps = void 0, this.depsTail = void 0, this.flags = 16, this.globalVersion = Le - 1, this.next = void 0, this.effect = this, this.__v_isReadonly = !t, this.isSSR = n;
	}
	notify() {
		if (this.flags |= 16, !(this.flags & 8) && ve !== this) return Ce(this, !0), !0;
	}
	get value() {
		let e = this.dep.track();
		return ke(this), e && (e.version = this.dep.version), this._value;
	}
	set value(e) {
		this.setter && this.setter(e);
	}
};
// @__NO_SIDE_EFFECTS__
function Gt(e, t, n = !1) {
	let r, i;
	return g(e) ? r = e : (r = e.get, i = e.set), new Wt(r, i, n);
}
var Kt = {}, qt = /* @__PURE__ */ new WeakMap(), Jt = void 0;
function Yt(e, t = !1, n = Jt) {
	if (n) {
		let t = qt.get(n);
		t || qt.set(n, t = []), t.push(e);
	}
}
function Xt(e, n, i = t) {
	let { immediate: a, deep: o, once: s, scheduler: l, augmentJob: u, call: f } = i, p = (e) => o ? e : /* @__PURE__ */ Mt(e) || o === !1 || o === 0 ? Zt(e, 1) : Zt(e), m, h, _, v, y = !1, b = !1;
	if (/* @__PURE__ */ Rt(e) ? (h = () => e.value, y = /* @__PURE__ */ Mt(e)) : /* @__PURE__ */ At(e) ? (h = () => p(e), y = !0) : d(e) ? (b = !0, y = e.some((e) => /* @__PURE__ */ At(e) || /* @__PURE__ */ Mt(e)), h = () => e.map((e) => {
		if (/* @__PURE__ */ Rt(e)) return e.value;
		if (/* @__PURE__ */ At(e)) return p(e);
		if (g(e)) return f ? f(e, 2) : e();
	})) : h = g(e) ? n ? f ? () => f(e, 2) : e : () => {
		if (_) {
			Pe();
			try {
				_();
			} finally {
				Fe();
			}
		}
		let t = Jt;
		Jt = m;
		try {
			return f ? f(e, 3, [v]) : e(v);
		} finally {
			Jt = t;
		}
	} : r, n && o) {
		let e = h, t = o === !0 ? Infinity : o;
		h = () => Zt(e(), t);
	}
	let x = _e(), S = () => {
		m.stop(), x && x.active && c(x.effects, m);
	};
	if (s && n) {
		let e = n;
		n = (...t) => {
			let n = e(...t);
			return S(), n;
		};
	}
	let C = b ? Array(e.length).fill(Kt) : Kt, w = (e) => {
		if (!(!(m.flags & 1) || !m.dirty && !e)) if (n) {
			let t = m.run();
			if (e || o || y || (b ? t.some((e, t) => P(e, C[t])) : P(t, C))) {
				_ && _();
				let e = Jt;
				Jt = m;
				try {
					let e = [
						t,
						C === Kt ? void 0 : b && C[0] === Kt ? [] : C,
						v
					];
					C = t, f ? f(n, 3, e) : n(...e);
				} finally {
					Jt = e;
				}
			}
		} else m.run();
	};
	return u && u(w), m = new be(h), m.scheduler = l ? () => l(w, !1) : w, v = (e) => Yt(e, !1, m), _ = m.onStop = () => {
		let e = qt.get(m);
		if (e) {
			if (f) f(e, 4);
			else for (let t of e) t();
			qt.delete(m);
		}
	}, n ? a ? w(!0) : C = m.run() : l ? l(w.bind(null, !0), !0) : m.run(), S.pause = m.pause.bind(m), S.resume = m.resume.bind(m), S.stop = S, S;
}
function Zt(e, t = Infinity, n) {
	if (t <= 0 || !y(e) || e.__v_skip || (n ||= /* @__PURE__ */ new Map(), (n.get(e) || 0) >= t)) return e;
	if (n.set(e, t), t--, /* @__PURE__ */ Rt(e)) Zt(e.value, t, n);
	else if (d(e)) for (let r = 0; r < e.length; r++) Zt(e[r], t, n);
	else if (p(e) || f(e)) e.forEach((e) => {
		Zt(e, t, n);
	});
	else if (w(e)) {
		for (let r in e) Zt(e[r], t, n);
		for (let r of Object.getOwnPropertySymbols(e)) Object.prototype.propertyIsEnumerable.call(e, r) && Zt(e[r], t, n);
	}
	return e;
}
//#endregion
//#region node_modules/@vue/runtime-core/dist/runtime-core.esm-bundler.js
function Qt(e, t, n, r) {
	try {
		return r ? e(...r) : e();
	} catch (e) {
		en(e, t, n);
	}
}
function $t(e, t, n, r) {
	if (g(e)) {
		let i = Qt(e, t, n, r);
		return i && b(i) && i.catch((e) => {
			en(e, t, n);
		}), i;
	}
	if (d(e)) {
		let i = [];
		for (let a = 0; a < e.length; a++) i.push($t(e[a], t, n, r));
		return i;
	}
}
function en(e, n, r, i = !0) {
	let a = n ? n.vnode : null, { errorHandler: o, throwUnhandledErrorInProduction: s } = n && n.appContext.config || t;
	if (n) {
		let t = n.parent, i = n.proxy, a = `https://vuejs.org/error-reference/#runtime-${r}`;
		for (; t;) {
			let n = t.ec;
			if (n) {
				for (let t = 0; t < n.length; t++) if (n[t](e, i, a) === !1) return;
			}
			t = t.parent;
		}
		if (o) {
			Pe(), Qt(o, null, 10, [
				e,
				i,
				a
			]), Fe();
			return;
		}
	}
	tn(e, r, a, i, s);
}
function tn(e, t, n, r = !0, i = !1) {
	if (i) throw e;
	console.error(e);
}
var nn = [], rn = -1, an = [], on = null, sn = 0, cn = /* @__PURE__ */ Promise.resolve(), ln = null;
function un(e) {
	let t = ln || cn;
	return e ? t.then(this ? e.bind(this) : e) : t;
}
function dn(e) {
	let t = rn + 1, n = nn.length;
	for (; t < n;) {
		let r = t + n >>> 1, i = nn[r], a = _n(i);
		a < e || a === e && i.flags & 2 ? t = r + 1 : n = r;
	}
	return t;
}
function fn(e) {
	if (!(e.flags & 1)) {
		let t = _n(e), n = nn[nn.length - 1];
		!n || !(e.flags & 2) && t >= _n(n) ? nn.push(e) : nn.splice(dn(t), 0, e), e.flags |= 1, pn();
	}
}
function pn() {
	ln ||= cn.then(vn);
}
function mn(e) {
	d(e) ? an.push(...e) : on && e.id === -1 ? on.splice(sn + 1, 0, e) : e.flags & 1 || (an.push(e), e.flags |= 1), pn();
}
function hn(e, t, n = rn + 1) {
	for (; n < nn.length; n++) {
		let t = nn[n];
		if (t && t.flags & 2) {
			if (e && t.id !== e.uid) continue;
			nn.splice(n, 1), n--, t.flags & 4 && (t.flags &= -2), t(), t.flags & 4 || (t.flags &= -2);
		}
	}
}
function gn(e) {
	if (an.length) {
		let e = [...new Set(an)].sort((e, t) => _n(e) - _n(t));
		if (an.length = 0, on) {
			on.push(...e);
			return;
		}
		for (on = e, sn = 0; sn < on.length; sn++) {
			let e = on[sn];
			e.flags & 4 && (e.flags &= -2), e.flags & 8 || e(), e.flags &= -2;
		}
		on = null, sn = 0;
	}
}
var _n = (e) => e.id == null ? e.flags & 2 ? -1 : Infinity : e.id;
function vn(e) {
	try {
		for (rn = 0; rn < nn.length; rn++) {
			let e = nn[rn];
			e && !(e.flags & 8) && (e.flags & 4 && (e.flags &= -2), Qt(e, e.i, e.i ? 15 : 14), e.flags & 4 || (e.flags &= -2));
		}
	} finally {
		for (; rn < nn.length; rn++) {
			let e = nn[rn];
			e && (e.flags &= -2);
		}
		rn = -1, nn.length = 0, gn(e), ln = null, (nn.length || an.length) && vn(e);
	}
}
var yn = null, bn = null;
function xn(e) {
	let t = yn;
	return yn = e, bn = e && e.type.__scopeId || null, t;
}
function Sn(e, t = yn, n) {
	if (!t || e._n) return e;
	let r = (...n) => {
		r._d && la(-1);
		let i = xn(t), a = aa.length, o;
		try {
			o = e(...n);
		} finally {
			for (let e = aa.length; e > a; e--) sa();
			xn(i), r._d && la(1);
		}
		return o;
	};
	return r._n = !0, r._c = !0, r._d = !0, r;
}
function W(e, n) {
	if (yn === null) return e;
	let r = Ga(yn), i = e.dirs ||= [];
	for (let e = 0; e < n.length; e++) {
		let [a, o, s, c = t] = n[e];
		a && (g(a) && (a = {
			mounted: a,
			updated: a
		}), a.deep && Zt(o), i.push({
			dir: a,
			instance: r,
			value: o,
			oldValue: void 0,
			arg: s,
			modifiers: c
		}));
	}
	return e;
}
function Cn(e, t, n, r) {
	let i = e.dirs, a = t && t.dirs;
	for (let o = 0; o < i.length; o++) {
		let s = i[o];
		a && (s.oldValue = a[o].value);
		let c = s.dir[r];
		c && (Pe(), $t(c, n, 8, [
			e.el,
			s,
			e,
			t
		]), Fe());
	}
}
function wn(e, t) {
	if (ka) {
		let n = ka.provides, r = ka.parent && ka.parent.provides;
		r === n && (n = ka.provides = Object.create(r)), n[e] = t;
	}
}
function Tn(e, t, n = !1) {
	let r = Aa();
	if (r || fi) {
		let i = fi ? fi._context.provides : r ? r.parent == null || r.ce ? r.vnode.appContext && r.vnode.appContext.provides : r.parent.provides : void 0;
		if (i && e in i) return i[e];
		if (arguments.length > 1) return n && g(t) ? t.call(r && r.proxy) : t;
	}
}
var En = /* @__PURE__ */ Symbol.for("v-scx"), Dn = () => Tn(En);
function On(e, t, n) {
	return kn(e, t, n);
}
function kn(e, n, i = t) {
	let { immediate: a, deep: o, flush: c, once: l } = i, u = s({}, i), d = n && a || !n && c !== "post", f;
	if (Ia) {
		if (c === "sync") {
			let e = Dn();
			f = e.__watcherHandles ||= [];
		} else if (!d) {
			let e = () => {};
			return e.stop = r, e.resume = r, e.pause = r, e;
		}
	}
	let p = ka;
	u.call = (e, t, n) => $t(e, p, t, n);
	let m = !1;
	c === "post" ? u.scheduler = (e) => {
		Ui(e, p && p.suspense);
	} : c !== "sync" && (m = !0, u.scheduler = (e, t) => {
		t ? e() : fn(e);
	}), u.augmentJob = (e) => {
		n && (e.flags |= 4), m && (e.flags |= 2, p && (e.id = p.uid, e.i = p));
	};
	let h = Xt(e, n, u);
	return Ia && (f ? f.push(h) : d && h()), h;
}
function An(e, t, n) {
	let r = this.proxy, i = _(e) ? e.includes(".") ? jn(r, e) : () => r[e] : e.bind(r, r), a;
	g(t) ? a = t : (a = t.handler, n = t);
	let o = Na(this), s = kn(i, a.bind(r), n);
	return o(), s;
}
function jn(e, t) {
	let n = t.split(".");
	return () => {
		let t = e;
		for (let e = 0; e < n.length && t; e++) t = t[n[e]];
		return t;
	};
}
var Mn = /* @__PURE__ */ new WeakMap(), Nn = /* @__PURE__ */ Symbol("_vte"), Pn = (e) => e.__isTeleport, Fn = (e) => e && (e.disabled || e.disabled === ""), In = (e) => e && (e.defer || e.defer === ""), Ln = (e) => typeof SVGElement < "u" && e instanceof SVGElement, Rn = (e) => typeof MathMLElement == "function" && e instanceof MathMLElement, zn = (e, t) => {
	let n = e && e.to;
	return _(n) ? t ? t(n) : null : n;
}, Bn = {
	name: "Teleport",
	__isTeleport: !0,
	process(e, t, n, r, i, a, o, s, c, l) {
		let { mc: u, pc: d, pbc: f, o: { insert: p, querySelector: m, createText: h, createComment: g, parentNode: _ } } = l, v = Fn(t.props), { dynamicChildren: y } = t, b = (e, t, n) => {
			e.shapeFlag & 16 && u(e.children, t, n, i, a, o, s, c);
		}, x = (e = t) => {
			let n = Fn(e.props), r = e.target = zn(e.props, m), a = Gn(r, e, h, p);
			r && (o !== "svg" && Ln(r) ? o = "svg" : o !== "mathml" && Rn(r) && (o = "mathml"), i && i.isCE && (i.ce._teleportTargets || (i.ce._teleportTargets = /* @__PURE__ */ new Set())).add(r), n || (b(e, r, a), Wn(e, !1)));
		}, S = (e) => {
			let t = () => {
				if (Mn.get(e) === t) {
					if (Mn.delete(e), Fn(e.props)) {
						let t = _(e.el) || n;
						b(e, t, e.anchor), Wn(e, !0);
					}
					x(e);
				}
			};
			Mn.set(e, t), Ui(t, a);
		};
		if (e == null) {
			let e = t.el = h(""), i = t.anchor = h("");
			if (p(e, n, r), p(i, n, r), In(t.props) || a && a.pendingBranch) {
				S(t);
				return;
			}
			v && (b(t, n, i), Wn(t, !0)), x();
		} else {
			t.el = e.el;
			let r = t.anchor = e.anchor, u = Mn.get(e);
			if (u) {
				u.flags |= 8, Mn.delete(e), S(t);
				return;
			}
			t.targetStart = e.targetStart;
			let p = t.target = e.target, h = t.targetAnchor = e.targetAnchor, g = Fn(e.props), _ = g ? n : p, b = g ? r : h;
			if (o === "svg" || Ln(p) ? o = "svg" : (o === "mathml" || Rn(p)) && (o = "mathml"), y ? (f(e.dynamicChildren, y, _, i, a, o, s), Yi(e, t, !0)) : c || d(e, t, _, b, i, a, o, s, !1), v) g ? t.props && e.props && t.props.to !== e.props.to && (t.props.to = e.props.to) : Vn(t, n, r, l, 1);
			else if ((t.props && t.props.to) !== (e.props && e.props.to)) {
				let e = zn(t.props, m);
				e && (t.target = e, Vn(t, e, null, l, 0));
			} else g && Vn(t, p, h, l, 1);
			Wn(t, v);
		}
	},
	remove(e, t, n, { um: r, o: { remove: i } }, a) {
		let { shapeFlag: o, children: s, anchor: c, targetStart: l, targetAnchor: u, target: d, props: f } = e, p = Fn(f), m = a || !p, h = Mn.get(e);
		if (h && (h.flags |= 8, Mn.delete(e)), d && (i(l), i(u)), a && i(c), !h && (p || d) && o & 16) for (let e = 0; e < s.length; e++) {
			let i = s[e];
			r(i, t, n, m, !!i.dynamicChildren);
		}
	},
	move: Vn,
	hydrate: Hn
};
function Vn(e, t, n, { o: { insert: r }, m: i }, a = 2) {
	a === 0 && r(e.targetAnchor, t, n);
	let { el: o, anchor: s, shapeFlag: c, children: l, props: u } = e, d = a === 2;
	if (d && r(o, t, n), !Mn.has(e) && (!d || Fn(u)) && c & 16) for (let e = 0; e < l.length; e++) i(l[e], t, n, 2);
	d && r(s, t, n);
}
function Hn(e, t, n, r, i, a, { o: { nextSibling: o, parentNode: s, querySelector: c, insert: l, createText: u } }, d) {
	function f(e, n) {
		let r = n;
		for (; r;) {
			if (r && r.nodeType === 8) {
				if (r.data === "teleport start anchor") t.targetStart = r;
				else if (r.data === "teleport anchor") {
					t.targetAnchor = r, e._lpa = t.targetAnchor && o(t.targetAnchor);
					break;
				}
			}
			r = o(r);
		}
	}
	function p(e, t) {
		t.anchor = d(o(e), t, s(e), n, r, i, a);
	}
	let m = t.target = zn(t.props, c), h = Fn(t.props);
	if (m) {
		let c = m._lpa || m.firstChild;
		t.shapeFlag & 16 && (h ? (p(e, t), f(m, c), t.targetAnchor || Gn(m, t, u, l, s(e) === m ? e : null)) : (t.anchor = o(e), f(m, c), t.targetAnchor || Gn(m, t, u, l), d(c && o(c), t, m, n, r, i, a))), Wn(t, h);
	} else h && t.shapeFlag & 16 && (p(e, t), t.targetStart = e, t.targetAnchor = o(e));
	return t.anchor && o(t.anchor);
}
var Un = Bn;
function Wn(e, t) {
	let n = e.ctx;
	if (n && n.ut) {
		let r, i;
		for (t ? (r = e.el, i = e.anchor) : (r = e.targetStart, i = e.targetAnchor); r && r !== i;) r.nodeType === 1 && r.setAttribute("data-v-owner", n.uid), r = r.nextSibling;
		n.ut();
	}
}
function Gn(e, t, n, r, i = null) {
	let a = t.targetStart = n(""), o = t.targetAnchor = n("");
	return a[Nn] = o, e && (r(a, e, i), r(o, e, i)), o;
}
var Kn = /* @__PURE__ */ Symbol("_leaveCb"), qn = /* @__PURE__ */ Symbol("_enterCb");
function Jn() {
	let e = {
		isMounted: !1,
		isLeaving: !1,
		isUnmounting: !1,
		leavingVNodes: /* @__PURE__ */ new Map()
	};
	return Er(() => {
		e.isMounted = !0;
	}), kr(() => {
		e.isUnmounting = !0;
	}), e;
}
var Yn = [Function, Array], Xn = {
	mode: String,
	appear: Boolean,
	persisted: Boolean,
	onBeforeEnter: Yn,
	onEnter: Yn,
	onAfterEnter: Yn,
	onEnterCancelled: Yn,
	onBeforeLeave: Yn,
	onLeave: Yn,
	onAfterLeave: Yn,
	onLeaveCancelled: Yn,
	onBeforeAppear: Yn,
	onAppear: Yn,
	onAfterAppear: Yn,
	onAppearCancelled: Yn
}, Zn = (e) => {
	let t = e.subTree;
	return t.component ? Zn(t.component) : t;
}, Qn = {
	name: "BaseTransition",
	props: Xn,
	setup(e, { slots: t }) {
		let n = Aa(), r = Jn();
		return () => {
			let i = t.default && or(t.default(), !0), a = i && i.length ? $n(i) : n.subTree ? Z() : void 0;
			if (!a) return;
			let o = /* @__PURE__ */ Pt(e), { mode: s } = o;
			if (r.isLeaving) return rr(a);
			let c = ir(a);
			if (!c) return rr(a);
			let l = nr(c, o, r, n, (e) => l = e);
			c.type !== ra && ar(c, l);
			let u = n.subTree && ir(n.subTree);
			if (u && u.type !== ra && !pa(u, c) && Zn(n).type !== ra) {
				let e = nr(u, o, r, n);
				if (ar(u, e), s === "out-in" && c.type !== ra) return r.isLeaving = !0, e.afterLeave = () => {
					r.isLeaving = !1, n.job.flags & 8 || n.update(), delete e.afterLeave, u = void 0;
				}, rr(a);
				s === "in-out" && c.type !== ra ? e.delayLeave = (e, t, n) => {
					let i = tr(r, u);
					i[String(u.key)] = u, e[Kn] = () => {
						t(), e[Kn] = void 0, delete l.delayedLeave, u = void 0;
					}, l.delayedLeave = () => {
						n(), delete l.delayedLeave, u = void 0;
					};
				} : u = void 0;
			} else u &&= void 0;
			return a;
		};
	}
};
function $n(e) {
	let t = e[0];
	if (e.length > 1) {
		for (let n of e) if (n.type !== ra) {
			t = n;
			break;
		}
	}
	return t;
}
var er = Qn;
function tr(e, t) {
	let { leavingVNodes: n } = e, r = n.get(t.type);
	return r || (r = /* @__PURE__ */ Object.create(null), n.set(t.type, r)), r;
}
function nr(e, t, n, r, i) {
	let { appear: a, mode: o, persisted: s = !1, onBeforeEnter: c, onEnter: l, onAfterEnter: u, onEnterCancelled: f, onBeforeLeave: p, onLeave: m, onAfterLeave: h, onLeaveCancelled: g, onBeforeAppear: _, onAppear: v, onAfterAppear: y, onAppearCancelled: b } = t, x = String(e.key), S = tr(n, e), C = (e, t) => {
		e && $t(e, r, 9, t);
	}, w = (e, t) => {
		let n = t[1];
		C(e, t), d(e) ? e.every((e) => e.length <= 1) && n() : e.length <= 1 && n();
	}, T = {
		mode: o,
		persisted: s,
		beforeEnter(t) {
			let r = c;
			if (!n.isMounted) if (a) r = _ || c;
			else return;
			t[Kn] && t[Kn](!0);
			let i = S[x];
			i && pa(e, i) && i.el[Kn] && i.el[Kn](), C(r, [t]);
		},
		enter(t) {
			if (S[x] === e) return;
			let r = l, i = u, o = f;
			if (!n.isMounted) if (a) r = v || l, i = y || u, o = b || f;
			else return;
			let s = !1;
			t[qn] = (e) => {
				s || (s = !0, C(e ? o : i, [t]), T.delayedLeave && T.delayedLeave(), t[qn] = void 0);
			};
			let c = t[qn].bind(null, !1);
			r ? w(r, [t, c]) : c();
		},
		leave(t, r) {
			let i = String(e.key);
			if (t[qn] && t[qn](!0), n.isUnmounting) return r();
			C(p, [t]);
			let a = !1;
			t[Kn] = (n) => {
				a || (a = !0, r(), C(n ? g : h, [t]), t[Kn] = void 0, S[i] === e && delete S[i]);
			};
			let o = t[Kn].bind(null, !1);
			S[i] = e, m ? w(m, [t, o]) : o();
		},
		clone(e) {
			let a = nr(e, t, n, r, i);
			return i && i(a), a;
		}
	};
	return T;
}
function rr(e) {
	if (mr(e)) return e = ya(e), e.children = null, e;
}
function ir(e) {
	if (!mr(e)) return Pn(e.type) && e.children ? $n(e.children) : e;
	if (e.component) return e.component.subTree;
	let { shapeFlag: t, children: n } = e;
	if (n) {
		if (t & 16) return n[0];
		if (t & 32 && g(n.default)) return n.default();
	}
}
function ar(e, t) {
	e.shapeFlag & 6 && e.component ? (e.transition = t, ar(e.component.subTree, t)) : e.shapeFlag & 128 ? (e.ssContent.transition = t.clone(e.ssContent), e.ssFallback.transition = t.clone(e.ssFallback)) : e.transition = t;
}
function or(e, t = !1, n) {
	let r = [], i = 0;
	for (let a = 0; a < e.length; a++) {
		let o = e[a], s = n == null ? o.key : String(n) + String(o.key == null ? a : o.key);
		o.type === K ? (o.patchFlag & 128 && i++, r = r.concat(or(o.children, t, s))) : (t || o.type !== ra) && r.push(s == null ? o : ya(o, { key: s }));
	}
	if (i > 1) for (let e = 0; e < r.length; e++) r[e].patchFlag = -2;
	return r;
}
// @__NO_SIDE_EFFECTS__
function sr(e, t) {
	return g(e) ? /* @__PURE__ */ s({ name: e.name }, t, { setup: e }) : e;
}
function cr(e) {
	e.ids = [
		e.ids[0] + e.ids[2]++ + "-",
		0,
		0
	];
}
function lr(e, t) {
	let n;
	return !!((n = Object.getOwnPropertyDescriptor(e, t)) && !n.configurable);
}
var ur = /* @__PURE__ */ new WeakMap();
function dr(e, n, r, a, o = !1) {
	if (d(e)) {
		e.forEach((e, t) => dr(e, n && (d(n) ? n[t] : n), r, a, o));
		return;
	}
	if (pr(a) && !o) {
		a.shapeFlag & 512 && a.type.__asyncResolved && a.component.subTree.component && dr(e, n, r, a.component.subTree);
		return;
	}
	let s = a.shapeFlag & 4 ? Ga(a.component) : a.el, l = o ? null : s, { i: f, r: p } = e, m = n && n.r, h = f.refs === t ? f.refs = {} : f.refs, v = f.setupState, y = /* @__PURE__ */ Pt(v), b = v === t ? i : (e) => !lr(h, e) && u(y, e), x = (e, t) => !(t && lr(h, t));
	if (m != null && m !== p) {
		if (fr(n), _(m)) h[m] = null, b(m) && (v[m] = null);
		else if (/* @__PURE__ */ Rt(m)) {
			let e = n;
			x(m, e.k) && (m.value = null), e.k && (h[e.k] = null);
		}
	}
	if (g(p)) Qt(p, f, 12, [l, h]);
	else {
		let t = _(p), n = /* @__PURE__ */ Rt(p);
		if (t || n) {
			let i = () => {
				if (e.f) {
					let n = t ? b(p) ? v[p] : h[p] : x(p) || !e.k ? p.value : h[e.k];
					if (o) d(n) && c(n, s);
					else if (d(n)) n.includes(s) || n.push(s);
					else if (t) h[p] = [s], b(p) && (v[p] = h[p]);
					else {
						let t = [s];
						x(p, e.k) && (p.value = t), e.k && (h[e.k] = t);
					}
				} else t ? (h[p] = l, b(p) && (v[p] = l)) : n && (x(p, e.k) && (p.value = l), e.k && (h[e.k] = l));
			};
			if (l) {
				let t = () => {
					i(), ur.delete(e);
				};
				t.id = -1, ur.set(e, t), Ui(t, r);
			} else fr(e), i();
		}
	}
}
function fr(e) {
	let t = ur.get(e);
	t && (t.flags |= 8, ur.delete(e));
}
L().requestIdleCallback, L().cancelIdleCallback;
var pr = (e) => !!e.type.__asyncLoader, mr = (e) => e.type.__isKeepAlive, hr = {
	name: "KeepAlive",
	__isKeepAlive: !0,
	props: {
		include: [
			String,
			RegExp,
			Array
		],
		exclude: [
			String,
			RegExp,
			Array
		],
		max: [String, Number]
	},
	setup(e, { slots: t }) {
		let n = Aa(), r = n.ctx;
		if (!r.renderer) return () => {
			let e = t.default && t.default();
			return e && e.length === 1 ? e[0] : e;
		};
		let i = /* @__PURE__ */ new Map(), a = /* @__PURE__ */ new Set(), o = null, s = n.suspense, { renderer: { p: c, m: l, um: u, o: { createElement: d } } } = r, f = d("div");
		r.activate = (e, t, n, r, i) => {
			let a = e.component;
			l(e, t, n, 0, s), c(a.vnode, e, t, n, a, s, r, e.slotScopeIds, i), Ui(() => {
				a.isDeactivated = !1, a.a && F(a.a);
				let t = e.props && e.props.onVnodeMounted;
				t && Ta(t, a.parent, e);
			}, s);
		}, r.deactivate = (e) => {
			let t = e.component;
			Qi(t.m), Qi(t.a), l(e, f, null, 1, s), Ui(() => {
				t.da && F(t.da);
				let n = e.props && e.props.onVnodeUnmounted;
				n && Ta(n, t.parent, e), t.isDeactivated = !0;
			}, s);
		};
		function p(e) {
			xr(e), u(e, n, s, !0);
		}
		function m(e) {
			i.forEach((t, n) => {
				let r = Ka(pr(t) ? t.type.__asyncResolved || {} : t.type);
				r && !e(r) && h(n);
			});
		}
		function h(e) {
			let t = i.get(e);
			t && (!o || !pa(t, o)) ? p(t) : o && xr(o), i.delete(e), a.delete(e);
		}
		On(() => [e.include, e.exclude], ([e, t]) => {
			e && m((t) => gr(e, t)), t && m((e) => !gr(t, e));
		}, {
			flush: "post",
			deep: !0
		});
		let g = null, _ = () => {
			g != null && (ea(n.subTree.type) ? Ui(() => {
				i.set(g, Sr(n.subTree));
			}, n.subTree.suspense) : i.set(g, Sr(n.subTree)));
		};
		return Er(_), Or(_), kr(() => {
			i.forEach((e) => {
				let { subTree: t, suspense: r } = n, i = Sr(t);
				if (e.type === i.type && e.key === i.key) {
					xr(i);
					let e = i.component.da;
					e && Ui(e, r);
					return;
				}
				p(e);
			});
		}), () => {
			if (g = null, !t.default) return o = null;
			let n = t.default(), r = n[0];
			if (n.length > 1) return o = null, n;
			if (!fa(r) || !(r.shapeFlag & 4) && !(r.shapeFlag & 128)) return o = null, r;
			let s = Sr(r);
			if (s.type === ra) return o = null, s;
			let c = s.type, l = Ka(pr(s) ? s.type.__asyncResolved || {} : c), { include: u, exclude: d, max: f } = e;
			if (u && (!l || !gr(u, l)) || d && l && gr(d, l)) return s.shapeFlag &= -257, o = s, r;
			let p = s.key == null ? c : s.key, m = i.get(p);
			return s.el && (s = ya(s), r.shapeFlag & 128 && (r.ssContent = s)), g = p, m ? (s.el = m.el, s.component = m.component, s.transition && ar(s, s.transition), s.shapeFlag |= 512, a.delete(p), a.add(p)) : (a.add(p), f && a.size > parseInt(f, 10) && h(a.values().next().value)), s.shapeFlag |= 256, o = s, ea(r.type) ? r : s;
		};
	}
};
function gr(e, t) {
	return d(e) ? e.some((e) => gr(e, t)) : _(e) ? e.split(",").includes(t) : h(e) ? (e.lastIndex = 0, e.test(t)) : !1;
}
function _r(e, t) {
	yr(e, "a", t);
}
function vr(e, t) {
	yr(e, "da", t);
}
function yr(e, t, n = ka) {
	let r = e.__wdc ||= () => {
		let t = n;
		for (; t;) {
			if (t.isDeactivated) return;
			t = t.parent;
		}
		return e();
	};
	if (Cr(t, r, n), n) {
		let e = n.parent;
		for (; e && e.parent;) mr(e.parent.vnode) && br(r, t, n, e), e = e.parent;
	}
}
function br(e, t, n, r) {
	let i = Cr(t, e, r, !0);
	Ar(() => {
		c(r[t], i);
	}, n);
}
function xr(e) {
	e.shapeFlag &= -257, e.shapeFlag &= -513;
}
function Sr(e) {
	return e.shapeFlag & 128 ? e.ssContent : e;
}
function Cr(e, t, n = ka, r = !1) {
	if (n) {
		let i = n[e] || (n[e] = []), a = t.__weh ||= (...r) => {
			Pe();
			let i = Na(n), a = $t(t, n, e, r);
			return i(), Fe(), a;
		};
		return r ? i.unshift(a) : i.push(a), a;
	}
}
var wr = (e) => (t, n = ka) => {
	(!Ia || e === "sp") && Cr(e, (...e) => t(...e), n);
}, Tr = wr("bm"), Er = wr("m"), Dr = wr("bu"), Or = wr("u"), kr = wr("bum"), Ar = wr("um"), jr = wr("sp"), Mr = wr("rtg"), Nr = wr("rtc");
function Pr(e, t = ka) {
	Cr("ec", e, t);
}
var Fr = "components";
function Ir(e, t) {
	return zr(Fr, e, !0, t) || e;
}
var Lr = /* @__PURE__ */ Symbol.for("v-ndc");
function Rr(e) {
	return _(e) ? zr(Fr, e, !1) || e : e || Lr;
}
function zr(e, t, n = !0, r = !1) {
	let i = yn || ka;
	if (i) {
		let n = i.type;
		if (e === Fr) {
			let e = Ka(n, !1);
			if (e && (e === t || e === k(t) || e === M(k(t)))) return n;
		}
		let a = Br(i[e] || n[e], t) || Br(i.appContext[e], t);
		return !a && r ? n : a;
	}
}
function Br(e, t) {
	return e && (e[t] || e[k(t)] || e[M(k(t))]);
}
function G(e, t, n, r) {
	let i, a = n && n[r], o = d(e);
	if (o || _(e)) {
		let n = o && /* @__PURE__ */ At(e), r = !1, s = !1;
		n && (r = !/* @__PURE__ */ Mt(e), s = /* @__PURE__ */ jt(e), e = Je(e)), i = Array(e.length);
		for (let n = 0, o = e.length; n < o; n++) i[n] = t(r ? s ? Lt(It(e[n])) : It(e[n]) : e[n], n, void 0, a && a[n]);
	} else if (typeof e == "number") {
		i = Array(e);
		for (let n = 0; n < e; n++) i[n] = t(n + 1, n, void 0, a && a[n]);
	} else if (y(e)) if (e[Symbol.iterator]) i = Array.from(e, (e, n) => t(e, n, void 0, a && a[n]));
	else {
		let n = Object.keys(e);
		i = Array(n.length);
		for (let r = 0, o = n.length; r < o; r++) {
			let o = n[r];
			i[r] = t(e[o], o, r, a && a[r]);
		}
	}
	else i = [];
	return n && (n[r] = i), i;
}
function Vr(e, t, n = {}, r, i, a) {
	if (yn.ce || yn.parent && pr(yn.parent) && yn.parent.ce) {
		let e = a != null && n.key == null ? s({}, n, { key: a }) : n, i = Object.keys(e).length > 0;
		return t !== "default" && (e.name = t), q(), da(K, null, [ga("slot", e, r && r())], i ? -2 : 64);
	}
	let o = e[t];
	o && o._c && (o._d = !1);
	let c = aa.length;
	q();
	let l;
	try {
		let i = o && Hr(o(n)), s = n.key || a || i && i.key;
		l = da(K, { key: (s && !v(s) ? s : `_${t}`) + (!i && r ? "_fb" : "") }, i || (r ? r() : []), i && e._ === 1 ? 64 : -2);
	} catch (e) {
		for (let e = aa.length; e > c; e--) sa();
		throw e;
	} finally {
		o && o._c && (o._d = !0);
	}
	return !i && l.scopeId && (l.slotScopeIds = [l.scopeId + "-s"]), l;
}
function Hr(e) {
	return e.some((e) => !fa(e) || !(e.type === ra || e.type === K && !Hr(e.children))) ? e : null;
}
var Ur = (e) => e ? Fa(e) ? Ga(e) : Ur(e.parent) : null, Wr = /* @__PURE__ */ s(/* @__PURE__ */ Object.create(null), {
	$: (e) => e,
	$el: (e) => e.vnode.el,
	$data: (e) => e.data,
	$props: (e) => e.props,
	$attrs: (e) => e.attrs,
	$slots: (e) => e.slots,
	$refs: (e) => e.refs,
	$parent: (e) => Ur(e.parent),
	$root: (e) => Ur(e.root),
	$host: (e) => e.ce,
	$emit: (e) => e.emit,
	$options: (e) => $r(e),
	$forceUpdate: (e) => e.f ||= () => {
		fn(e.update);
	},
	$nextTick: (e) => e.n ||= un.bind(e.proxy),
	$watch: (e) => An.bind(e)
}), Gr = (e, n) => e !== t && !e.__isScriptSetup && u(e, n), Kr = {
	get({ _: e }, n) {
		if (n === "__v_skip") return !0;
		let { ctx: r, setupState: i, data: a, props: o, accessCache: s, type: c, appContext: l } = e;
		if (n[0] !== "$") {
			let e = s[n];
			if (e !== void 0) switch (e) {
				case 1: return i[n];
				case 2: return a[n];
				case 4: return r[n];
				case 3: return o[n];
			}
			else if (Gr(i, n)) return s[n] = 1, i[n];
			else if (a !== t && u(a, n)) return s[n] = 2, a[n];
			else if (u(o, n)) return s[n] = 3, o[n];
			else if (r !== t && u(r, n)) return s[n] = 4, r[n];
			else Jr && (s[n] = 0);
		}
		let d = Wr[n], f, p;
		if (d) return n === "$attrs" && Ge(e.attrs, "get", ""), d(e);
		if ((f = c.__cssModules) && (f = f[n])) return f;
		if (r !== t && u(r, n)) return s[n] = 4, r[n];
		if (p = l.config.globalProperties, u(p, n)) return p[n];
	},
	set({ _: e }, n, r) {
		let { data: i, setupState: a, ctx: o } = e;
		return Gr(a, n) ? (a[n] = r, !0) : i !== t && u(i, n) ? (i[n] = r, !0) : u(e.props, n) || n[0] === "$" && n.slice(1) in e ? !1 : (o[n] = r, !0);
	},
	has({ _: { data: e, setupState: n, accessCache: r, ctx: i, appContext: a, props: o, type: s } }, c) {
		let l;
		return !!(r[c] || e !== t && c[0] !== "$" && u(e, c) || Gr(n, c) || u(o, c) || u(i, c) || u(Wr, c) || u(a.config.globalProperties, c) || (l = s.__cssModules) && l[c]);
	},
	defineProperty(e, t, n) {
		return n.get == null ? u(n, "value") && this.set(e, t, n.value, null) : e._.accessCache[t] = 0, Reflect.defineProperty(e, t, n);
	}
};
function qr(e) {
	return d(e) ? e.reduce((e, t) => (e[t] = null, e), {}) : e;
}
var Jr = !0;
function Yr(e) {
	let t = $r(e), n = e.proxy, i = e.ctx;
	Jr = !1, t.beforeCreate && Zr(t.beforeCreate, e, "bc");
	let { data: a, computed: o, methods: s, watch: c, provide: l, inject: u, created: f, beforeMount: p, mounted: m, beforeUpdate: h, updated: _, activated: v, deactivated: b, beforeDestroy: x, beforeUnmount: S, destroyed: C, unmounted: w, render: T, renderTracked: E, renderTriggered: D, errorCaptured: O, serverPrefetch: k, expose: A, inheritAttrs: j, components: M, directives: N, filters: P } = t;
	if (u && Xr(u, i, null), s) for (let e in s) {
		let t = s[e];
		g(t) && (i[e] = t.bind(n));
	}
	if (a) {
		let t = a.call(n, n);
		y(t) && (e.data = /* @__PURE__ */ Et(t));
	}
	if (Jr = !0, o) for (let e in o) {
		let t = o[e], a = Q({
			get: g(t) ? t.bind(n, n) : g(t.get) ? t.get.bind(n, n) : r,
			set: !g(t) && g(t.set) ? t.set.bind(n) : r
		});
		Object.defineProperty(i, e, {
			enumerable: !0,
			configurable: !0,
			get: () => a.value,
			set: (e) => a.value = e
		});
	}
	if (c) for (let e in c) Qr(c[e], i, n, e);
	if (l) {
		let e = g(l) ? l.call(n) : l;
		Reflect.ownKeys(e).forEach((t) => {
			wn(t, e[t]);
		});
	}
	f && Zr(f, e, "c");
	function F(e, t) {
		d(t) ? t.forEach((t) => e(t.bind(n))) : t && e(t.bind(n));
	}
	if (F(Tr, p), F(Er, m), F(Dr, h), F(Or, _), F(_r, v), F(vr, b), F(Pr, O), F(Nr, E), F(Mr, D), F(kr, S), F(Ar, w), F(jr, k), d(A)) if (A.length) {
		let t = e.exposed ||= {};
		A.forEach((e) => {
			Object.defineProperty(t, e, {
				get: () => n[e],
				set: (t) => n[e] = t,
				enumerable: !0
			});
		});
	} else e.exposed ||= {};
	T && e.render === r && (e.render = T), j != null && (e.inheritAttrs = j), M && (e.components = M), N && (e.directives = N), k && cr(e);
}
function Xr(e, t, n = r) {
	d(e) && (e = ii(e));
	for (let n in e) {
		let r = e[n], i;
		i = y(r) ? "default" in r ? Tn(r.from || n, r.default, !0) : Tn(r.from || n) : Tn(r), /* @__PURE__ */ Rt(i) ? Object.defineProperty(t, n, {
			enumerable: !0,
			configurable: !0,
			get: () => i.value,
			set: (e) => i.value = e
		}) : t[n] = i;
	}
}
function Zr(e, t, n) {
	$t(d(e) ? e.map((e) => e.bind(t.proxy)) : e.bind(t.proxy), t, n);
}
function Qr(e, t, n, r) {
	let i = r.includes(".") ? jn(n, r) : () => n[r];
	if (_(e)) {
		let n = t[e];
		g(n) && On(i, n);
	} else if (g(e)) On(i, e.bind(n));
	else if (y(e)) if (d(e)) e.forEach((e) => Qr(e, t, n, r));
	else {
		let r = g(e.handler) ? e.handler.bind(n) : t[e.handler];
		g(r) && On(i, r, e);
	}
}
function $r(e) {
	let t = e.type, { mixins: n, extends: r } = t, { mixins: i, optionsCache: a, config: { optionMergeStrategies: o } } = e.appContext, s = a.get(t), c;
	return s ? c = s : !i.length && !n && !r ? c = t : (c = {}, i.length && i.forEach((e) => ei(c, e, o, !0)), ei(c, t, o)), y(t) && a.set(t, c), c;
}
function ei(e, t, n, r = !1) {
	let { mixins: i, extends: a } = t;
	a && ei(e, a, n, !0), i && i.forEach((t) => ei(e, t, n, !0));
	for (let i in t) if (!(r && i === "expose")) {
		let r = ti[i] || n && n[i];
		e[i] = r ? r(e[i], t[i]) : t[i];
	}
	return e;
}
var ti = {
	data: ni,
	props: si,
	emits: si,
	methods: oi,
	computed: oi,
	beforeCreate: ai,
	created: ai,
	beforeMount: ai,
	mounted: ai,
	beforeUpdate: ai,
	updated: ai,
	beforeDestroy: ai,
	beforeUnmount: ai,
	destroyed: ai,
	unmounted: ai,
	activated: ai,
	deactivated: ai,
	errorCaptured: ai,
	serverPrefetch: ai,
	components: oi,
	directives: oi,
	watch: ci,
	provide: ni,
	inject: ri
};
function ni(e, t) {
	return t ? e ? function() {
		return s(g(e) ? e.call(this, this) : e, g(t) ? t.call(this, this) : t);
	} : t : e;
}
function ri(e, t) {
	return oi(ii(e), ii(t));
}
function ii(e) {
	if (d(e)) {
		let t = {};
		for (let n = 0; n < e.length; n++) t[e[n]] = e[n];
		return t;
	}
	return e;
}
function ai(e, t) {
	return e ? [...new Set([].concat(e, t))] : t;
}
function oi(e, t) {
	return e ? s(/* @__PURE__ */ Object.create(null), e, t) : t;
}
function si(e, t) {
	return e ? d(e) && d(t) ? [.../* @__PURE__ */ new Set([...e, ...t])] : s(/* @__PURE__ */ Object.create(null), qr(e), qr(t ?? {})) : t;
}
function ci(e, t) {
	if (!e) return t;
	if (!t) return e;
	let n = s(/* @__PURE__ */ Object.create(null), e);
	for (let r in t) n[r] = ai(e[r], t[r]);
	return n;
}
function li() {
	return {
		app: null,
		config: {
			isNativeTag: i,
			performance: !1,
			globalProperties: {},
			optionMergeStrategies: {},
			errorHandler: void 0,
			warnHandler: void 0,
			compilerOptions: {}
		},
		mixins: [],
		components: {},
		directives: {},
		provides: /* @__PURE__ */ Object.create(null),
		optionsCache: /* @__PURE__ */ new WeakMap(),
		propsCache: /* @__PURE__ */ new WeakMap(),
		emitsCache: /* @__PURE__ */ new WeakMap()
	};
}
var ui = 0;
function di(e, t) {
	return function(n, r = null) {
		g(n) || (n = s({}, n)), r != null && !y(r) && (r = null);
		let i = li(), a = /* @__PURE__ */ new WeakSet(), o = [], c = !1, l = i.app = {
			_uid: ui++,
			_component: n,
			_props: r,
			_container: null,
			_context: i,
			_instance: null,
			version: Ya,
			get config() {
				return i.config;
			},
			set config(e) {},
			use(e, ...t) {
				return a.has(e) || (e && g(e.install) ? (a.add(e), e.install(l, ...t)) : g(e) && (a.add(e), e(l, ...t))), l;
			},
			mixin(e) {
				return i.mixins.includes(e) || i.mixins.push(e), l;
			},
			component(e, t) {
				return t ? (i.components[e] = t, l) : i.components[e];
			},
			directive(e, t) {
				return t ? (i.directives[e] = t, l) : i.directives[e];
			},
			mount(a, o, s) {
				if (!c) {
					let u = l._ceVNode || ga(n, r);
					return u.appContext = i, s === !0 ? s = "svg" : s === !1 && (s = void 0), o && t ? t(u, a) : e(u, a, s), c = !0, l._container = a, a.__vue_app__ = l, Ga(u.component);
				}
			},
			onUnmount(e) {
				o.push(e);
			},
			unmount() {
				c && ($t(o, l._instance, 16), e(null, l._container), delete l._container.__vue_app__);
			},
			provide(e, t) {
				return i.provides[e] = t, l;
			},
			runWithContext(e) {
				let t = fi;
				fi = l;
				try {
					return e();
				} finally {
					fi = t;
				}
			}
		};
		return l;
	};
}
var fi = null, pi = (e, t) => t === "modelValue" || t === "model-value" ? e.modelModifiers : e[`${t}Modifiers`] || e[`${k(t)}Modifiers`] || e[`${j(t)}Modifiers`];
function mi(e, n, ...r) {
	if (e.isUnmounted) return;
	let i = e.vnode.props || t, a = r, o = n.startsWith("update:"), s = o && pi(i, n.slice(7));
	s && (s.trim && (a = r.map((e) => _(e) ? e.trim() : e)), s.number && (a = r.map(ee)));
	let c, l = i[c = N(n)] || i[c = N(k(n))];
	!l && o && (l = i[c = N(j(n))]), l && $t(l, e, 6, a);
	let u = i[c + "Once"];
	if (u) {
		if (!e.emitted) e.emitted = {};
		else if (e.emitted[c]) return;
		e.emitted[c] = !0, $t(u, e, 6, a);
	}
}
var hi = /* @__PURE__ */ new WeakMap();
function gi(e, t, n = !1) {
	let r = n ? hi : t.emitsCache, i = r.get(e);
	if (i !== void 0) return i;
	let a = e.emits, o = {}, c = !1;
	if (!g(e)) {
		let r = (e) => {
			let n = gi(e, t, !0);
			n && (c = !0, s(o, n));
		};
		!n && t.mixins.length && t.mixins.forEach(r), e.extends && r(e.extends), e.mixins && e.mixins.forEach(r);
	}
	return !a && !c ? (y(e) && r.set(e, null), null) : (d(a) ? a.forEach((e) => o[e] = null) : s(o, a), y(e) && r.set(e, o), o);
}
function _i(e, t) {
	return !e || !a(t) ? !1 : (t = t.slice(2), t = t === "Once" ? t : t.replace(/Once$/, ""), u(e, t[0].toLowerCase() + t.slice(1)) || u(e, j(t)) || u(e, t));
}
function vi(e) {
	let { type: t, vnode: n, proxy: r, withProxy: i, propsOptions: [a], slots: s, attrs: c, emit: l, render: u, renderCache: d, props: f, data: p, setupState: m, ctx: h, inheritAttrs: g } = e, _ = xn(e), v, y;
	try {
		if (n.shapeFlag & 4) {
			let e = i || r, t = e;
			v = xa(u.call(t, e, d, f, m, p, h)), y = c;
		} else {
			let e = t;
			v = xa(e.length > 1 ? e(f, {
				attrs: c,
				slots: s,
				emit: l
			}) : e(f, null)), y = t.props ? c : yi(c);
		}
	} catch (t) {
		aa.length = 0, en(t, e, 1), v = ga(ra);
	}
	let b = v;
	if (y && g !== !1) {
		let e = Object.keys(y), { shapeFlag: t } = b;
		e.length && t & 7 && (a && e.some(o) && (y = bi(y, a)), b = ya(b, y, !1, !0));
	}
	return n.dirs && (b = ya(b, null, !1, !0), b.dirs = b.dirs ? b.dirs.concat(n.dirs) : n.dirs), n.transition && ar(b, n.transition), v = b, xn(_), v;
}
var yi = (e) => {
	let t;
	for (let n in e) (n === "class" || n === "style" || a(n)) && ((t ||= {})[n] = e[n]);
	return t;
}, bi = (e, t) => {
	let n = {};
	for (let r in e) (!o(r) || !(r.slice(9) in t)) && (n[r] = e[r]);
	return n;
};
function xi(e, t, n) {
	let { props: r, children: i, component: a } = e, { props: o, children: s, patchFlag: c } = t, l = a.emitsOptions;
	if (t.dirs || t.transition) return !0;
	if (n && c >= 0) {
		if (c & 1024) return !0;
		if (c & 16) return r ? Si(r, o, l) : !!o;
		if (c & 8) {
			let e = t.dynamicProps;
			for (let t = 0; t < e.length; t++) {
				let n = e[t];
				if (Ci(o, r, n) && !_i(l, n)) return !0;
			}
		}
	} else return (i || s) && (!s || !s.$stable) ? !0 : r === o ? !1 : r ? !o || Si(r, o, l) : !!o;
	return !1;
}
function Si(e, t, n) {
	let r = Object.keys(t);
	if (r.length !== Object.keys(e).length) return !0;
	for (let i = 0; i < r.length; i++) {
		let a = r[i];
		if (Ci(t, e, a) && !_i(n, a)) return !0;
	}
	return !1;
}
function Ci(e, t, n) {
	let r = e[n], i = t[n];
	return n === "style" && y(r) && y(i) ? !ue(r, i) : r !== i;
}
function wi({ vnode: e, parent: t, suspense: n }, r) {
	for (; t;) {
		let n = t.subTree;
		if (n.suspense && n.suspense.activeBranch === e && (n.suspense.vnode.el = n.el = r, e = n), n === e) (e = t.vnode).el = r, t = t.parent;
		else break;
	}
	n && n.activeBranch === e && (n.vnode.el = r);
}
var Ti = {}, Ei = () => Object.create(Ti), Di = (e) => Object.getPrototypeOf(e) === Ti;
function Oi(e, t, n, r = !1) {
	let i = {}, a = Ei();
	e.propsDefaults = /* @__PURE__ */ Object.create(null), Ai(e, t, i, a);
	for (let t in e.propsOptions[0]) t in i || (i[t] = void 0);
	e.props = n ? r ? i : /* @__PURE__ */ Dt(i) : e.type.props ? i : a, e.attrs = a;
}
function ki(e, t, n, r) {
	let { props: i, attrs: a, vnode: { patchFlag: o } } = e, s = /* @__PURE__ */ Pt(i), [c] = e.propsOptions, l = !1;
	if ((r || o > 0) && !(o & 16)) {
		if (o & 8) {
			let n = e.vnode.dynamicProps;
			for (let r = 0; r < n.length; r++) {
				let o = n[r];
				if (_i(e.emitsOptions, o)) continue;
				let d = t[o];
				if (c) if (u(a, o)) d !== a[o] && (a[o] = d, l = !0);
				else {
					let t = k(o);
					i[t] = ji(c, s, t, d, e, !1);
				}
				else d !== a[o] && (a[o] = d, l = !0);
			}
		}
	} else {
		Ai(e, t, i, a) && (l = !0);
		let r;
		for (let a in s) (!t || !u(t, a) && ((r = j(a)) === a || !u(t, r))) && (c ? n && (n[a] !== void 0 || n[r] !== void 0) && (i[a] = ji(c, s, a, void 0, e, !0)) : delete i[a]);
		if (a !== s) for (let e in a) (!t || !u(t, e)) && (delete a[e], l = !0);
	}
	l && Ke(e.attrs, "set", "");
}
function Ai(e, n, r, i) {
	let [a, o] = e.propsOptions, s = !1, c;
	if (n) for (let t in n) {
		if (E(t)) continue;
		let l = n[t], d;
		a && u(a, d = k(t)) ? !o || !o.includes(d) ? r[d] = l : (c ||= {})[d] = l : _i(e.emitsOptions, t) || (!(t in i) || l !== i[t]) && (i[t] = l, s = !0);
	}
	if (o) {
		let n = /* @__PURE__ */ Pt(r), i = c || t;
		for (let t = 0; t < o.length; t++) {
			let s = o[t];
			r[s] = ji(a, n, s, i[s], e, !u(i, s));
		}
	}
	return s;
}
function ji(e, t, n, r, i, a) {
	let o = e[n];
	if (o != null) {
		let e = u(o, "default");
		if (e && r === void 0) {
			let e = o.default;
			if (o.type !== Function && !o.skipFactory && g(e)) {
				let { propsDefaults: a } = i;
				if (n in a) r = a[n];
				else {
					let o = Na(i);
					r = a[n] = e.call(null, t), o();
				}
			} else r = e;
			i.ce && i.ce._setProp(n, r);
		}
		o[0] && (a && !e ? r = !1 : o[1] && (r === "" || r === j(n)) && (r = !0));
	}
	return r;
}
var Mi = /* @__PURE__ */ new WeakMap();
function Ni(e, r, i = !1) {
	let a = i ? Mi : r.propsCache, o = a.get(e);
	if (o) return o;
	let c = e.props, l = {}, f = [], p = !1;
	if (!g(e)) {
		let t = (e) => {
			p = !0;
			let [t, n] = Ni(e, r, !0);
			s(l, t), n && f.push(...n);
		};
		!i && r.mixins.length && r.mixins.forEach(t), e.extends && t(e.extends), e.mixins && e.mixins.forEach(t);
	}
	if (!c && !p) return y(e) && a.set(e, n), n;
	if (d(c)) for (let e = 0; e < c.length; e++) {
		let n = k(c[e]);
		Pi(n) && (l[n] = t);
	}
	else if (c) for (let e in c) {
		let t = k(e);
		if (Pi(t)) {
			let n = c[e], r = l[t] = d(n) || g(n) ? { type: n } : s({}, n), i = r.type, a = !1, o = !0;
			if (d(i)) for (let e = 0; e < i.length; ++e) {
				let t = i[e], n = g(t) && t.name;
				if (n === "Boolean") {
					a = !0;
					break;
				}
				n === "String" && (o = !1);
			}
			else a = g(i) && i.name === "Boolean";
			r[0] = a, r[1] = o, (a || u(r, "default")) && f.push(t);
		}
	}
	let m = [l, f];
	return y(e) && a.set(e, m), m;
}
function Pi(e) {
	return e[0] !== "$" && !E(e);
}
var Fi = (e) => e === "_" || e === "_ctx" || e === "$stable", Ii = (e) => d(e) ? e.map(xa) : [xa(e)], Li = (e, t, n) => {
	if (t._n) return t;
	let r = Sn((...e) => Ii(t(...e)), n);
	return r._c = !1, r;
}, Ri = (e, t, n) => {
	let r = e._ctx;
	for (let n in e) {
		if (Fi(n)) continue;
		let i = e[n];
		if (g(i)) t[n] = Li(n, i, r);
		else if (i != null) {
			let e = Ii(i);
			t[n] = () => e;
		}
	}
}, zi = (e, t) => {
	let n = Ii(t);
	e.slots.default = () => n;
}, Bi = (e, t, n) => {
	for (let r in t) (n || !Fi(r)) && (e[r] = t[r]);
}, Vi = (e, t, n) => {
	let r = e.slots = Ei();
	if (e.vnode.shapeFlag & 32) {
		let e = t._;
		e ? (Bi(r, t, n), n && I(r, "_", e, !0)) : Ri(t, r);
	} else t && zi(e, t);
}, Hi = (e, n, r) => {
	let { vnode: i, slots: a } = e, o = !0, s = t;
	if (i.shapeFlag & 32) {
		let e = n._;
		e ? r && e === 1 ? o = !1 : Bi(a, n, r) : (o = !n.$stable, Ri(n, a)), s = n;
	} else n && (zi(e, n), s = { default: 1 });
	if (o) for (let e in a) !Fi(e) && s[e] == null && delete a[e];
}, Ui = ta;
function Wi(e) {
	return Gi(e);
}
function Gi(e, i) {
	let a = L();
	a.__VUE__ = !0;
	let { insert: o, remove: s, patchProp: c, createElement: l, createText: u, createComment: d, setText: f, setElementText: p, parentNode: m, nextSibling: h, setScopeId: g = r, insertStaticContent: _ } = e, v = (e, t, n, r = null, i = null, a = null, o = void 0, s = null, c = !!t.dynamicChildren) => {
		if (e === t) return;
		e && !pa(e, t) && (r = ce(e), ie(e, i, a, !0), e = null), t.patchFlag === -2 && (c = !1, t.dynamicChildren = null);
		let { type: l, ref: u, shapeFlag: d } = t;
		switch (l) {
			case na:
				y(e, t, n, r);
				break;
			case ra:
				b(e, t, n, r);
				break;
			case ia:
				e ?? x(t, n, r, o);
				break;
			case K:
				M(e, t, n, r, i, a, o, s, c);
				break;
			default: d & 1 ? w(e, t, n, r, i, a, o, s, c) : d & 6 ? N(e, t, n, r, i, a, o, s, c) : (d & 64 || d & 128) && l.process(e, t, n, r, i, a, o, s, c, de);
		}
		u != null && i ? dr(u, e && e.ref, a, t || e, !t) : u == null && e && e.ref != null && dr(e.ref, null, a, e, !0);
	}, y = (e, t, n, r) => {
		if (e == null) o(t.el = u(t.children), n, r);
		else {
			let n = t.el = e.el;
			t.children !== e.children && f(n, t.children);
		}
	}, b = (e, t, n, r) => {
		e == null ? o(t.el = d(t.children || ""), n, r) : t.el = e.el;
	}, x = (e, t, n, r) => {
		[e.el, e.anchor] = _(e.children, t, n, r, e.el, e.anchor);
	}, S = ({ el: e, anchor: t }, n, r) => {
		let i;
		for (; e && e !== t;) i = h(e), o(e, n, r), e = i;
		o(t, n, r);
	}, C = ({ el: e, anchor: t }) => {
		let n;
		for (; e && e !== t;) n = h(e), s(e), e = n;
		s(t);
	}, w = (e, t, n, r, i, a, o, s, c) => {
		if (t.type === "svg" ? o = "svg" : t.type === "math" && (o = "mathml"), e == null) T(t, n, r, i, a, o, s, c);
		else {
			let n = e.el && e.el._isVueCE ? e.el : null;
			try {
				n && n._beginPatch(), k(e, t, i, a, o, s, c);
			} finally {
				n && n._endPatch();
			}
		}
	}, T = (e, t, n, r, i, a, s, u) => {
		let d, f, { props: m, shapeFlag: h, transition: g, dirs: _ } = e;
		if (d = e.el = l(e.type, a, m && m.is, m), h & 8 ? p(d, e.children) : h & 16 && O(e.children, d, null, r, i, Ki(e, a), s, u), _ && Cn(e, null, r, "created"), D(d, e, e.scopeId, s, r), m) {
			for (let e in m) e !== "value" && !E(e) && c(d, e, null, m[e], a, r);
			"value" in m && c(d, "value", null, m.value, a), (f = m.onVnodeBeforeMount) && Ta(f, r, e);
		}
		_ && Cn(e, null, r, "beforeMount");
		let v = Ji(i, g);
		v && g.beforeEnter(d), o(d, t, n), ((f = m && m.onVnodeMounted) || v || _) && Ui(() => {
			try {
				f && Ta(f, r, e), v && g.enter(d), _ && Cn(e, null, r, "mounted");
			} finally {}
		}, i);
	}, D = (e, t, n, r, i) => {
		if (n && g(e, n), r) for (let t = 0; t < r.length; t++) g(e, r[t]);
		if (i) {
			let n = i.subTree;
			if (t === n || ea(n.type) && (n.ssContent === t || n.ssFallback === t)) {
				let t = i.vnode;
				D(e, t, t.scopeId, t.slotScopeIds, i.parent);
			}
		}
	}, O = (e, t, n, r, i, a, o, s, c = 0) => {
		for (let l = c; l < e.length; l++) {
			let c = e[l] = s ? Sa(e[l]) : xa(e[l]);
			v(null, c, t, n, r, i, a, o, s);
		}
	}, k = (e, n, r, i, a, o, s) => {
		let l = n.el = e.el, { patchFlag: u, dynamicChildren: d, dirs: f } = n;
		u |= e.patchFlag & 16;
		let m = e.props || t, h = n.props || t, g;
		if (r && qi(r, !1), (g = h.onVnodeBeforeUpdate) && Ta(g, r, n, e), f && Cn(n, e, r, "beforeUpdate"), r && qi(r, !0), d && (!e.dynamicChildren || e.dynamicChildren.length !== d.length) && (u = 0, s = !1, d = null), (m.innerHTML && h.innerHTML == null || m.textContent && h.textContent == null) && p(l, ""), d ? A(e.dynamicChildren, d, l, r, i, Ki(n, a), o) : s || ne(e, n, l, null, r, i, Ki(n, a), o, !1), u > 0) {
			if (u & 16) j(l, m, h, r, a);
			else if (u & 2 && m.class !== h.class && c(l, "class", null, h.class, a), u & 4 && c(l, "style", m.style, h.style, a), u & 8) {
				let e = n.dynamicProps;
				for (let t = 0; t < e.length; t++) {
					let n = e[t], i = m[n], o = h[n];
					(o !== i || n === "value") && c(l, n, i, o, a, r);
				}
			}
			u & 1 && e.children !== n.children && p(l, n.children);
		} else !s && d == null && j(l, m, h, r, a);
		((g = h.onVnodeUpdated) || f) && Ui(() => {
			g && Ta(g, r, n, e), f && Cn(n, e, r, "updated");
		}, i);
	}, A = (e, t, n, r, i, a, o) => {
		for (let s = 0; s < t.length; s++) {
			let c = e[s], l = t[s], u = c.el && (c.type === K || !pa(c, l) || c.shapeFlag & 198) ? m(c.el) : n;
			v(c, l, u, null, r, i, a, o, !0);
		}
	}, j = (e, n, r, i, a) => {
		if (n !== r) {
			if (n !== t) for (let t in n) !E(t) && !(t in r) && c(e, t, n[t], null, a, i);
			for (let t in r) {
				if (E(t)) continue;
				let o = r[t], s = n[t];
				o !== s && t !== "value" && c(e, t, s, o, a, i);
			}
			"value" in r && c(e, "value", n.value, r.value, a);
		}
	}, M = (e, t, n, r, i, a, s, c, l) => {
		let d = t.el = e ? e.el : u(""), f = t.anchor = e ? e.anchor : u(""), { patchFlag: p, dynamicChildren: m, slotScopeIds: h } = t;
		h && (c = c ? c.concat(h) : h), e == null ? (o(d, n, r), o(f, n, r), O(t.children || [], n, f, i, a, s, c, l)) : p > 0 && p & 64 && m && e.dynamicChildren && e.dynamicChildren.length === m.length ? (A(e.dynamicChildren, m, n, i, a, s, c), (t.key != null || i && t === i.subTree) && Yi(e, t, !0)) : ne(e, t, n, f, i, a, s, c, l);
	}, N = (e, t, n, r, i, a, o, s, c) => {
		t.slotScopeIds = s, e == null ? t.shapeFlag & 512 ? i.ctx.activate(t, n, r, o, c) : P(t, n, r, i, a, o, c) : I(e, t, c);
	}, P = (e, t, n, r, i, a, o) => {
		let s = e.component = Oa(e, r, i);
		if (mr(e) && (s.ctx.renderer = de), La(s, !1, o), s.asyncDep) {
			if (i && i.registerDep(s, ee, o), !e.el) {
				let r = s.subTree = ga(ra);
				b(null, r, t, n), e.placeholder = r.el;
			}
		} else ee(s, e, t, n, i, a, o);
	}, I = (e, t, n) => {
		let r = t.component = e.component;
		if (xi(e, t, n)) if (r.asyncDep && !r.asyncResolved) {
			te(r, t, n);
			return;
		} else r.next = t, r.update();
		else t.el = e.el, r.vnode = t;
	}, ee = (e, t, n, r, i, a, o) => {
		let s = () => {
			if (e.isMounted) {
				let { next: t, bu: n, u: r, parent: s, vnode: c } = e;
				{
					let n = Zi(e);
					if (n) {
						t && (t.el = c.el, te(e, t, o)), n.asyncDep.then(() => {
							Ui(() => {
								e.isUnmounted || l();
							}, i);
						});
						return;
					}
				}
				let u = t, d;
				qi(e, !1), t ? (t.el = c.el, te(e, t, o)) : t = c, n && F(n), (d = t.props && t.props.onVnodeBeforeUpdate) && Ta(d, s, t, c), qi(e, !0);
				let f = vi(e), p = e.subTree;
				e.subTree = f, v(p, f, m(p.el), ce(p), e, i, a), t.el = f.el, u === null && wi(e, f.el), r && Ui(r, i), (d = t.props && t.props.onVnodeUpdated) && Ui(() => Ta(d, s, t, c), i);
			} else {
				let o, { el: s, props: c } = t, { bm: l, m: u, parent: d, root: f, type: p } = e, m = pr(t);
				if (qi(e, !1), l && F(l), !m && (o = c && c.onVnodeBeforeMount) && Ta(o, d, t), qi(e, !0), s && V) {
					let t = () => {
						e.subTree = vi(e), V(s, e.subTree, e, i, null);
					};
					m && p.__asyncHydrate ? p.__asyncHydrate(s, e, t) : t();
				} else {
					f.ce && f.ce._hasShadowRoot() && f.ce._injectChildStyle(p, e.parent ? e.parent.type : void 0);
					let o = e.subTree = vi(e);
					v(null, o, n, r, e, i, a), t.el = o.el;
				}
				if (u && Ui(u, i), !m && (o = c && c.onVnodeMounted)) {
					let e = t;
					Ui(() => Ta(o, d, e), i);
				}
				(t.shapeFlag & 256 || d && pr(d.vnode) && d.vnode.shapeFlag & 256) && e.a && Ui(e.a, i), e.isMounted = !0, t = n = r = null;
			}
		};
		e.scope.on();
		let c = e.effect = new be(s);
		e.scope.off();
		let l = e.update = c.run.bind(c), u = e.job = c.runIfDirty.bind(c);
		u.i = e, u.id = e.uid, c.scheduler = () => fn(u), qi(e, !0), l();
	}, te = (e, t, n) => {
		t.component = e;
		let r = e.vnode.props;
		e.vnode = t, e.next = null, ki(e, t.props, r, n), Hi(e, t.children, n), Pe(), hn(e), Fe();
	}, ne = (e, t, n, r, i, a, o, s, c = !1) => {
		let l = e && e.children, u = e ? e.shapeFlag : 0, d = t.children, { patchFlag: f, shapeFlag: m } = t;
		if (f > 0) {
			if (f & 128) {
				z(l, d, n, r, i, a, o, s, c);
				return;
			}
			if (f & 256) {
				R(l, d, n, r, i, a, o, s, c);
				return;
			}
		}
		m & 8 ? (u & 16 && se(l, i, a), d !== l && p(n, d)) : u & 16 ? m & 16 ? z(l, d, n, r, i, a, o, s, c) : se(l, i, a, !0) : (u & 8 && p(n, ""), m & 16 && O(d, n, r, i, a, o, s, c));
	}, R = (e, t, r, i, a, o, s, c, l) => {
		e ||= n, t ||= n;
		let u = e.length, d = t.length, f = Math.min(u, d), p;
		for (p = 0; p < f; p++) {
			let n = t[p] = l ? Sa(t[p]) : xa(t[p]);
			v(e[p], n, r, null, a, o, s, c, l);
		}
		u > d ? se(e, a, o, !0, !1, f) : O(t, r, i, a, o, s, c, l, f);
	}, z = (e, t, r, i, a, o, s, c, l) => {
		let u = 0, d = t.length, f = e.length - 1, p = d - 1;
		for (; u <= f && u <= p;) {
			let n = e[u], i = t[u] = l ? Sa(t[u]) : xa(t[u]);
			if (pa(n, i)) v(n, i, r, null, a, o, s, c, l);
			else break;
			u++;
		}
		for (; u <= f && u <= p;) {
			let n = e[f], i = t[p] = l ? Sa(t[p]) : xa(t[p]);
			if (pa(n, i)) v(n, i, r, null, a, o, s, c, l);
			else break;
			f--, p--;
		}
		if (u > f) {
			if (u <= p) {
				let e = p + 1, n = e < d ? t[e].el : i;
				for (; u <= p;) v(null, t[u] = l ? Sa(t[u]) : xa(t[u]), r, n, a, o, s, c, l), u++;
			}
		} else if (u > p) for (; u <= f;) ie(e[u], a, o, !0), u++;
		else {
			let m = u, h = u, g = /* @__PURE__ */ new Map();
			for (u = h; u <= p; u++) {
				let e = t[u] = l ? Sa(t[u]) : xa(t[u]);
				e.key != null && g.set(e.key, u);
			}
			let _, y = 0, b = p - h + 1, x = !1, S = 0, C = Array(b);
			for (u = 0; u < b; u++) C[u] = 0;
			for (u = m; u <= f; u++) {
				let n = e[u];
				if (y >= b) {
					ie(n, a, o, !0);
					continue;
				}
				let i;
				if (n.key != null) i = g.get(n.key);
				else for (_ = h; _ <= p; _++) if (C[_ - h] === 0 && pa(n, t[_])) {
					i = _;
					break;
				}
				i === void 0 ? ie(n, a, o, !0) : (C[i - h] = u + 1, i >= S ? S = i : x = !0, v(n, t[i], r, null, a, o, s, c, l), y++);
			}
			let w = x ? Xi(C) : n;
			for (_ = w.length - 1, u = b - 1; u >= 0; u--) {
				let e = h + u, n = t[e], f = t[e + 1], p = e + 1 < d ? f.el || $i(f) : i;
				C[u] === 0 ? v(null, n, r, p, a, o, s, c, l) : x && (_ < 0 || u !== w[_] ? re(n, r, p, 2) : _--);
			}
		}
	}, re = (e, t, n, r, i = null) => {
		let { el: a, type: c, transition: l, children: u, shapeFlag: d } = e;
		if (d & 6) {
			re(e.component.subTree, t, n, r);
			return;
		}
		if (d & 128) {
			e.suspense.move(t, n, r);
			return;
		}
		if (d & 64) {
			c.move(e, t, n, de);
			return;
		}
		if (c === K) {
			o(a, t, n);
			for (let e = 0; e < u.length; e++) re(u[e], t, n, r);
			o(e.anchor, t, n);
			return;
		}
		if (c === ia) {
			S(e, t, n);
			return;
		}
		if (r !== 2 && d & 1 && l) if (r === 0) l.persisted && !a[Kn] ? o(a, t, n) : (l.beforeEnter(a), o(a, t, n), Ui(() => l.enter(a), i));
		else {
			let { leave: r, delayLeave: i, afterLeave: c } = l, u = () => {
				e.ctx.isUnmounted ? s(a) : o(a, t, n);
			}, d = () => {
				let e = a._isLeaving || !!a[Kn];
				a._isLeaving && a[Kn](!0), l.persisted && !e ? u() : r(a, () => {
					u(), c && c();
				});
			};
			i ? i(a, u, d) : d();
		}
		else o(a, t, n);
	}, ie = (e, t, n, r = !1, i = !1) => {
		let { type: a, props: o, ref: s, children: c, dynamicChildren: l, shapeFlag: u, patchFlag: d, dirs: f, cacheIndex: p, memo: m } = e;
		if (d === -2 && (i = !1), s != null && (Pe(), dr(s, null, n, e, !0), Fe()), p != null && (t.renderCache[p] = void 0), u & 256) {
			t.ctx.deactivate(e);
			return;
		}
		let h = u & 1 && f, g = !pr(e), _;
		if (g && (_ = o && o.onVnodeBeforeUnmount) && Ta(_, t, e), u & 6) oe(e.component, n, r);
		else {
			if (u & 128) {
				e.suspense.unmount(n, r);
				return;
			}
			h && Cn(e, null, t, "beforeUnmount"), u & 64 ? e.type.remove(e, t, n, de, r) : l && !l.hasOnce && (a !== K || d > 0 && d & 64) ? se(l, t, n, !1, !0) : (a === K && d & 384 || !i && u & 16) && se(c, t, n), r && ae(e);
		}
		let v = m != null && p == null;
		(g && (_ = o && o.onVnodeUnmounted) || h || v) && Ui(() => {
			_ && Ta(_, t, e), h && Cn(e, null, t, "unmounted"), v && (e.el = null);
		}, n);
	}, ae = (e) => {
		let { type: t, el: n, anchor: r, transition: i } = e;
		if (t === K) {
			B(n, r);
			return;
		}
		if (t === ia) {
			C(e);
			return;
		}
		let a = () => {
			s(n), i && !i.persisted && i.afterLeave && i.afterLeave();
		};
		if (e.shapeFlag & 1 && i && !i.persisted) {
			let { leave: t, delayLeave: r } = i, o = () => t(n, a);
			r ? r(e.el, a, o) : o();
		} else a();
	}, B = (e, t) => {
		let n;
		for (; e !== t;) n = h(e), s(e), e = n;
		s(t);
	}, oe = (e, t, n) => {
		let { bum: r, scope: i, job: a, subTree: o, um: s, m: c, a: l } = e;
		Qi(c), Qi(l), r && F(r), i.stop(), a && (a.flags |= 8, ie(o, e, t, n)), s && Ui(s, t), Ui(() => {
			e.isUnmounted = !0;
		}, t);
	}, se = (e, t, n, r = !1, i = !1, a = 0) => {
		for (let o = a; o < e.length; o++) ie(e[o], t, n, r, i);
	}, ce = (e) => {
		if (e.shapeFlag & 6) return ce(e.component.subTree);
		if (e.shapeFlag & 128) return e.suspense.next();
		let t = h(e.anchor || e.el), n = t && t[Nn];
		return n ? h(n) : t;
	}, le = !1, ue = (e, t, n) => {
		let r;
		e == null ? t._vnode && (ie(t._vnode, null, null, !0), r = t._vnode.component) : v(t._vnode || null, e, t, null, null, null, n), t._vnode = e, le ||= (le = !0, hn(r), gn(), !1);
	}, de = {
		p: v,
		um: ie,
		m: re,
		r: ae,
		mt: P,
		mc: O,
		pc: ne,
		pbc: A,
		n: ce,
		o: e
	}, fe, V;
	return i && ([fe, V] = i(de)), {
		render: ue,
		hydrate: fe,
		createApp: di(ue, fe)
	};
}
function Ki({ type: e, props: t }, n) {
	return n === "svg" && e === "foreignObject" || n === "mathml" && e === "annotation-xml" && t && t.encoding && t.encoding.includes("html") ? void 0 : n;
}
function qi({ effect: e, job: t }, n) {
	n ? (e.flags |= 32, t.flags |= 4) : (e.flags &= -33, t.flags &= -5);
}
function Ji(e, t) {
	return (!e || e && !e.pendingBranch) && t && !t.persisted;
}
function Yi(e, t, n = !1) {
	let r = e.children, i = t.children;
	if (d(r) && d(i)) for (let e = 0; e < r.length; e++) {
		let t = r[e], a = i[e];
		a.shapeFlag & 1 && !a.dynamicChildren && ((a.patchFlag <= 0 || a.patchFlag === 32) && (a = i[e] = Sa(i[e]), a.el = t.el), !n && a.patchFlag !== -2 && Yi(t, a)), a.type === na && (a.patchFlag === -1 && (a = i[e] = Sa(a)), a.el = t.el), a.type === ra && !a.el && (a.el = t.el);
	}
}
function Xi(e) {
	let t = e.slice(), n = [0], r, i, a, o, s, c = e.length;
	for (r = 0; r < c; r++) {
		let c = e[r];
		if (c !== 0) {
			if (i = n[n.length - 1], e[i] < c) {
				t[r] = i, n.push(r);
				continue;
			}
			for (a = 0, o = n.length - 1; a < o;) s = a + o >> 1, e[n[s]] < c ? a = s + 1 : o = s;
			c < e[n[a]] && (a > 0 && (t[r] = n[a - 1]), n[a] = r);
		}
	}
	for (a = n.length, o = n[a - 1]; a-- > 0;) n[a] = o, o = t[o];
	return n;
}
function Zi(e) {
	let t = e.subTree.component;
	if (t) return t.asyncDep && !t.asyncResolved ? t : Zi(t);
}
function Qi(e) {
	if (e) for (let t = 0; t < e.length; t++) e[t].flags |= 8;
}
function $i(e) {
	if (e.placeholder) return e.placeholder;
	let t = e.component;
	return t ? $i(t.subTree) : null;
}
var ea = (e) => e.__isSuspense;
function ta(e, t) {
	t && t.pendingBranch ? d(e) ? t.effects.push(...e) : t.effects.push(e) : mn(e);
}
var K = /* @__PURE__ */ Symbol.for("v-fgt"), na = /* @__PURE__ */ Symbol.for("v-txt"), ra = /* @__PURE__ */ Symbol.for("v-cmt"), ia = /* @__PURE__ */ Symbol.for("v-stc"), aa = [], oa = null;
function q(e = !1) {
	aa.push(oa = e ? null : []);
}
function sa() {
	aa.pop(), oa = aa[aa.length - 1] || null;
}
var ca = 1;
function la(e, t = !1) {
	ca += e, e < 0 && oa && t && (oa.hasOnce = !0);
}
function ua(e) {
	return e.dynamicChildren = ca > 0 ? oa || n : null, sa(), ca > 0 && oa && oa.push(e), e;
}
function J(e, t, n, r, i, a) {
	return ua(Y(e, t, n, r, i, a, !0));
}
function da(e, t, n, r, i) {
	return ua(ga(e, t, n, r, i, !0));
}
function fa(e) {
	return e ? e.__v_isVNode === !0 : !1;
}
function pa(e, t) {
	return e.type === t.type && e.key === t.key;
}
var ma = ({ key: e }) => e ?? null, ha = ({ ref: e, ref_key: t, ref_for: n }) => (typeof e == "number" && (e = "" + e), e == null ? null : _(e) || /* @__PURE__ */ Rt(e) || g(e) ? {
	i: yn,
	r: e,
	k: t,
	f: !!n
} : e);
function Y(e, t = null, n = null, r = 0, i = null, a = e === K ? 0 : 1, o = !1, s = !1) {
	let c = {
		__v_isVNode: !0,
		__v_skip: !0,
		type: e,
		props: t,
		key: t && ma(t),
		ref: t && ha(t),
		scopeId: bn,
		slotScopeIds: null,
		children: n,
		component: null,
		suspense: null,
		ssContent: null,
		ssFallback: null,
		dirs: null,
		transition: null,
		el: null,
		anchor: null,
		target: null,
		targetStart: null,
		targetAnchor: null,
		staticCount: 0,
		shapeFlag: a,
		patchFlag: r,
		dynamicProps: i,
		dynamicChildren: null,
		appContext: null,
		ctx: yn
	};
	return s ? (Ca(c, n), a & 128 && e.normalize(c)) : n && (c.shapeFlag |= _(n) ? 8 : 16), ca > 0 && !o && oa && (c.patchFlag > 0 || a & 6) && c.patchFlag !== 32 && oa.push(c), c;
}
var ga = _a;
function _a(e, t = null, n = null, r = 0, i = null, a = !1) {
	if ((!e || e === Lr) && (e = ra), fa(e)) {
		let r = ya(e, t, !0);
		return n && Ca(r, n), ca > 0 && !a && oa && (r.shapeFlag & 6 ? oa[oa.indexOf(e)] = r : oa.push(r)), r.patchFlag = -2, r;
	}
	if (qa(e) && (e = e.__vccOpts), t) {
		t = va(t);
		let { class: e, style: n } = t;
		e && !_(e) && (t.class = B(e)), y(n) && (/* @__PURE__ */ Nt(n) && !d(n) && (n = s({}, n)), t.style = R(n));
	}
	let o = _(e) ? 1 : ea(e) ? 128 : Pn(e) ? 64 : y(e) ? 4 : g(e) ? 2 : 0;
	return Y(e, t, n, r, i, o, a, !0);
}
function va(e) {
	return e ? /* @__PURE__ */ Nt(e) || Di(e) ? s({}, e) : e : null;
}
function ya(e, t, n = !1, r = !1) {
	let { props: i, ref: a, patchFlag: o, children: s, transition: c } = e, l = t ? wa(i || {}, t) : i, u = {
		__v_isVNode: !0,
		__v_skip: !0,
		type: e.type,
		props: l,
		key: l && ma(l),
		ref: t && t.ref ? n && a ? d(a) ? a.concat(ha(t)) : [a, ha(t)] : ha(t) : a,
		scopeId: e.scopeId,
		slotScopeIds: e.slotScopeIds,
		children: s,
		target: e.target,
		targetStart: e.targetStart,
		targetAnchor: e.targetAnchor,
		staticCount: e.staticCount,
		shapeFlag: e.shapeFlag,
		patchFlag: t && e.type !== K ? o === -1 ? 16 : o | 16 : o,
		dynamicProps: e.dynamicProps,
		dynamicChildren: e.dynamicChildren,
		appContext: e.appContext,
		dirs: e.dirs,
		transition: c,
		component: e.component,
		suspense: e.suspense,
		ssContent: e.ssContent && ya(e.ssContent),
		ssFallback: e.ssFallback && ya(e.ssFallback),
		placeholder: e.placeholder,
		el: e.el,
		anchor: e.anchor,
		ctx: e.ctx,
		ce: e.ce
	};
	return c && r && ar(u, c.clone(u)), u;
}
function X(e = " ", t = 0) {
	return ga(na, null, e, t);
}
function ba(e, t) {
	let n = ga(ia, null, e);
	return n.staticCount = t, n;
}
function Z(e = "", t = !1) {
	return t ? (q(), da(ra, null, e)) : ga(ra, null, e);
}
function xa(e) {
	return e == null || typeof e == "boolean" ? ga(ra) : d(e) ? ga(K, null, e.slice()) : fa(e) ? Sa(e) : ga(na, null, String(e));
}
function Sa(e) {
	return e.el === null && e.patchFlag !== -1 || e.memo ? e : ya(e);
}
function Ca(e, t) {
	let n = 0, { shapeFlag: r } = e;
	if (t == null) t = null;
	else if (d(t)) n = 16;
	else if (typeof t == "object") if (r & 65) {
		let n = t.default;
		n && (n._c && (n._d = !1), Ca(e, n()), n._c && (n._d = !0));
		return;
	} else {
		n = 32;
		let r = t._;
		!r && !Di(t) ? t._ctx = yn : r === 3 && yn && (yn.slots._ === 1 ? t._ = 1 : (t._ = 2, e.patchFlag |= 1024));
	}
	else if (g(t)) {
		if (r & 65) {
			Ca(e, { default: t });
			return;
		}
		t = {
			default: t,
			_ctx: yn
		}, n = 32;
	} else t = String(t), r & 64 ? (n = 16, t = [X(t)]) : n = 8;
	e.children = t, e.shapeFlag |= n;
}
function wa(...e) {
	let t = {};
	for (let n = 0; n < e.length; n++) {
		let r = e[n];
		for (let e in r) if (e === "class") t.class !== r.class && (t.class = B([t.class, r.class]));
		else if (e === "style") t.style = R([t.style, r.style]);
		else if (a(e)) {
			let n = t[e], i = r[e];
			i && n !== i && !(d(n) && n.includes(i)) ? t[e] = n ? [].concat(n, i) : i : i == null && n == null && !o(e) && (t[e] = i);
		} else e !== "" && (t[e] = r[e]);
	}
	return t;
}
function Ta(e, t, n, r = null) {
	$t(e, t, 7, [n, r]);
}
var Ea = li(), Da = 0;
function Oa(e, n, r) {
	let i = e.type, a = (n ? n.appContext : e.appContext) || Ea, o = {
		uid: Da++,
		vnode: e,
		type: i,
		parent: n,
		appContext: a,
		root: null,
		next: null,
		subTree: null,
		effect: null,
		update: null,
		job: null,
		scope: new ge(!0),
		render: null,
		proxy: null,
		exposed: null,
		exposeProxy: null,
		withProxy: null,
		provides: n ? n.provides : Object.create(a.provides),
		ids: n ? n.ids : [
			"",
			0,
			0
		],
		accessCache: null,
		renderCache: [],
		components: null,
		directives: null,
		propsOptions: Ni(i, a),
		emitsOptions: gi(i, a),
		emit: null,
		emitted: null,
		propsDefaults: t,
		inheritAttrs: i.inheritAttrs,
		ctx: t,
		data: t,
		props: t,
		attrs: t,
		slots: t,
		refs: t,
		setupState: t,
		setupContext: null,
		suspense: r,
		suspenseId: r ? r.pendingId : 0,
		asyncDep: null,
		asyncResolved: !1,
		isMounted: !1,
		isUnmounted: !1,
		isDeactivated: !1,
		bc: null,
		c: null,
		bm: null,
		m: null,
		bu: null,
		u: null,
		um: null,
		bum: null,
		da: null,
		a: null,
		rtg: null,
		rtc: null,
		ec: null,
		sp: null
	};
	return o.ctx = { _: o }, o.root = n ? n.root : o, o.emit = mi.bind(null, o), e.ce && e.ce(o), o;
}
var ka = null, Aa = () => ka || yn, ja, Ma;
{
	let e = L(), t = (t, n) => {
		let r;
		return (r = e[t]) || (r = e[t] = []), r.push(n), (e) => {
			r.length > 1 ? r.forEach((t) => t(e)) : r[0](e);
		};
	};
	ja = t("__VUE_INSTANCE_SETTERS__", (e) => ka = e), Ma = t("__VUE_SSR_SETTERS__", (e) => Ia = e);
}
var Na = (e) => {
	let t = ka;
	return ja(e), e.scope.on(), () => {
		e.scope.off(), ja(t);
	};
}, Pa = () => {
	ka && ka.scope.off(), ja(null);
};
function Fa(e) {
	return e.vnode.shapeFlag & 4;
}
var Ia = !1;
function La(e, t = !1, n = !1) {
	t && Ma(t);
	let { props: r, children: i } = e.vnode, a = Fa(e);
	Oi(e, r, a, t), Vi(e, i, n || t);
	let o = a ? Ra(e, t) : void 0;
	return t && Ma(!1), o;
}
function Ra(e, t) {
	let n = e.type;
	e.accessCache = /* @__PURE__ */ Object.create(null), e.proxy = new Proxy(e.ctx, Kr);
	let { setup: r } = n;
	if (r) {
		Pe();
		let n = e.setupContext = r.length > 1 ? Wa(e) : null, i = Na(e), a = Qt(r, e, 0, [e.props, n]), o = b(a);
		if (Fe(), i(), (o || e.sp) && !pr(e) && cr(e), o) {
			if (a.then(Pa, Pa), t) return a.then((n) => {
				za(e, n, t);
			}).catch((t) => {
				en(t, e, 0);
			});
			e.asyncDep = a;
		} else za(e, a, t);
	} else Ha(e, t);
}
function za(e, t, n) {
	g(t) ? e.type.__ssrInlineRender ? e.ssrRender = t : e.render = t : y(t) && (e.setupState = Ut(t)), Ha(e, n);
}
var Ba, Va;
function Ha(e, t, n) {
	let i = e.type;
	if (!e.render) {
		if (!t && Ba && !i.render) {
			let t = i.template || $r(e).template;
			if (t) {
				let { isCustomElement: n, compilerOptions: r } = e.appContext.config, { delimiters: a, compilerOptions: o } = i;
				i.render = Ba(t, s(s({
					isCustomElement: n,
					delimiters: a
				}, r), o));
			}
		}
		e.render = i.render || r, Va && Va(e);
	}
	{
		let t = Na(e);
		Pe();
		try {
			Yr(e);
		} finally {
			Fe(), t();
		}
	}
}
var Ua = { get(e, t) {
	return Ge(e, "get", ""), e[t];
} };
function Wa(e) {
	return {
		attrs: new Proxy(e.attrs, Ua),
		slots: e.slots,
		emit: e.emit,
		expose: (t) => {
			e.exposed = t || {};
		}
	};
}
function Ga(e) {
	return e.exposed ? e.exposeProxy ||= new Proxy(Ut(Ft(e.exposed)), {
		get(t, n) {
			if (n in t) return t[n];
			if (n in Wr) return Wr[n](e);
		},
		has(e, t) {
			return t in e || t in Wr;
		}
	}) : e.proxy;
}
function Ka(e, t = !0) {
	return g(e) ? e.displayName || e.name : e.name || t && e.__name;
}
function qa(e) {
	return g(e) && "__vccOpts" in e;
}
var Q = (e, t) => /* @__PURE__ */ Gt(e, t, Ia);
function Ja(e, t, n) {
	try {
		la(-1);
		let r = arguments.length;
		return r === 2 ? y(t) && !d(t) ? fa(t) ? ga(e, null, [t]) : ga(e, t) : ga(e, null, t) : (r > 3 ? n = Array.prototype.slice.call(arguments, 2) : r === 3 && fa(n) && (n = [n]), ga(e, t, n));
	} finally {
		la(1);
	}
}
var Ya = "3.5.40", Xa = void 0, Za = typeof window < "u" && window.trustedTypes;
if (Za) try {
	Xa = /* @__PURE__ */ Za.createPolicy("vue", { createHTML: (e) => e });
} catch {}
var Qa = Xa ? (e) => Xa.createHTML(e) : (e) => e, $a = "http://www.w3.org/2000/svg", eo = "http://www.w3.org/1998/Math/MathML", to = typeof document < "u" ? document : null, no = to && /* @__PURE__ */ to.createElement("template"), ro = {
	insert: (e, t, n) => {
		t.insertBefore(e, n || null);
	},
	remove: (e) => {
		let t = e.parentNode;
		t && t.removeChild(e);
	},
	createElement: (e, t, n, r) => {
		let i = t === "svg" ? to.createElementNS($a, e) : t === "mathml" ? to.createElementNS(eo, e) : n ? to.createElement(e, { is: n }) : to.createElement(e);
		return e === "select" && r && r.multiple != null && i.setAttribute("multiple", r.multiple), i;
	},
	createText: (e) => to.createTextNode(e),
	createComment: (e) => to.createComment(e),
	setText: (e, t) => {
		e.nodeValue = t;
	},
	setElementText: (e, t) => {
		e.textContent = t;
	},
	parentNode: (e) => e.parentNode,
	nextSibling: (e) => e.nextSibling,
	querySelector: (e) => to.querySelector(e),
	setScopeId(e, t) {
		e.setAttribute(t, "");
	},
	insertStaticContent(e, t, n, r, i, a) {
		let o = n ? n.previousSibling : t.lastChild;
		if (i && (i === a || i.nextSibling)) for (; t.insertBefore(i.cloneNode(!0), n), !(i === a || !(i = i.nextSibling)););
		else {
			no.innerHTML = Qa(r === "svg" ? `<svg>${e}</svg>` : r === "mathml" ? `<math>${e}</math>` : e);
			let i = no.content;
			if (r === "svg" || r === "mathml") {
				let e = i.firstChild;
				for (; e.firstChild;) i.appendChild(e.firstChild);
				i.removeChild(e);
			}
			t.insertBefore(i, n);
		}
		return [o ? o.nextSibling : t.firstChild, n ? n.previousSibling : t.lastChild];
	}
}, io = "transition", ao = "animation", oo = /* @__PURE__ */ Symbol("_vtc"), so = {
	name: String,
	type: String,
	css: {
		type: Boolean,
		default: !0
	},
	duration: [
		String,
		Number,
		Object
	],
	enterFromClass: String,
	enterActiveClass: String,
	enterToClass: String,
	appearFromClass: String,
	appearActiveClass: String,
	appearToClass: String,
	leaveFromClass: String,
	leaveActiveClass: String,
	leaveToClass: String
}, co = /* @__PURE__ */ s({}, Xn, so), lo = /* @__PURE__ */ ((e) => (e.displayName = "Transition", e.props = co, e))((e, { slots: t }) => Ja(er, po(e), t)), uo = (e, t = []) => {
	d(e) ? e.forEach((e) => e(...t)) : e && e(...t);
}, fo = (e) => e ? d(e) ? e.some((e) => e.length > 1) : e.length > 1 : !1;
function po(e) {
	let t = {};
	for (let n in e) n in so || (t[n] = e[n]);
	if (e.css === !1) return t;
	let { name: n = "v", type: r, duration: i, enterFromClass: a = `${n}-enter-from`, enterActiveClass: o = `${n}-enter-active`, enterToClass: c = `${n}-enter-to`, appearFromClass: l = a, appearActiveClass: u = o, appearToClass: d = c, leaveFromClass: f = `${n}-leave-from`, leaveActiveClass: p = `${n}-leave-active`, leaveToClass: m = `${n}-leave-to` } = e, h = mo(i), g = h && h[0], _ = h && h[1], { onBeforeEnter: v, onEnter: y, onEnterCancelled: b, onLeave: x, onLeaveCancelled: S, onBeforeAppear: C = v, onAppear: w = y, onAppearCancelled: T = b } = t, E = (e, t, n, r) => {
		e._enterCancelled = r, _o(e, t ? d : c), _o(e, t ? u : o), n && n();
	}, D = (e, t) => {
		e._isLeaving = !1, _o(e, f), _o(e, m), _o(e, p), t && t();
	}, O = (e) => (t, n) => {
		let i = e ? w : y, o = () => E(t, e, n);
		uo(i, [t, o]), vo(() => {
			_o(t, e ? l : a), go(t, e ? d : c), fo(i) || bo(t, r, g, o);
		});
	};
	return s(t, {
		onBeforeEnter(e) {
			uo(v, [e]), go(e, a), go(e, o);
		},
		onBeforeAppear(e) {
			uo(C, [e]), go(e, l), go(e, u);
		},
		onEnter: O(!1),
		onAppear: O(!0),
		onLeave(e, t) {
			e._isLeaving = !0;
			let n = () => D(e, t);
			go(e, f), e._enterCancelled ? (go(e, p), wo(e)) : (wo(e), go(e, p)), vo(() => {
				e._isLeaving && (_o(e, f), go(e, m), fo(x) || bo(e, r, _, n));
			}), uo(x, [e, n]);
		},
		onEnterCancelled(e) {
			E(e, !1, void 0, !0), uo(b, [e]);
		},
		onAppearCancelled(e) {
			E(e, !0, void 0, !0), uo(T, [e]);
		},
		onLeaveCancelled(e) {
			D(e), uo(S, [e]);
		}
	});
}
function mo(e) {
	if (e == null) return null;
	if (y(e)) return [ho(e.enter), ho(e.leave)];
	{
		let t = ho(e);
		return [t, t];
	}
}
function ho(e) {
	return te(e);
}
function go(e, t) {
	t.split(/\s+/).forEach((t) => t && e.classList.add(t)), (e[oo] || (e[oo] = /* @__PURE__ */ new Set())).add(t);
}
function _o(e, t) {
	t.split(/\s+/).forEach((t) => t && e.classList.remove(t));
	let n = e[oo];
	n && (n.delete(t), n.size || (e[oo] = void 0));
}
function vo(e) {
	requestAnimationFrame(() => {
		requestAnimationFrame(e);
	});
}
var yo = 0;
function bo(e, t, n, r) {
	let i = e._endId = ++yo, a = () => {
		i === e._endId && r();
	};
	if (n != null) return setTimeout(a, n);
	let { type: o, timeout: s, propCount: c } = xo(e, t);
	if (!o) return r();
	let l = o + "end", u = 0, d = () => {
		e.removeEventListener(l, f), a();
	}, f = (t) => {
		t.target === e && ++u >= c && d();
	};
	setTimeout(() => {
		u < c && d();
	}, s + 1), e.addEventListener(l, f);
}
function xo(e, t) {
	let n = window.getComputedStyle(e), r = (e) => (n[e] || "").split(", "), i = r(`${io}Delay`), a = r(`${io}Duration`), o = So(i, a), s = r(`${ao}Delay`), c = r(`${ao}Duration`), l = So(s, c), u = null, d = 0, f = 0;
	t === io ? o > 0 && (u = io, d = o, f = a.length) : t === ao ? l > 0 && (u = ao, d = l, f = c.length) : (d = Math.max(o, l), u = d > 0 ? o > l ? io : ao : null, f = u ? u === io ? a.length : c.length : 0);
	let p = u === io && /\b(?:transform|all)(?:,|$)/.test(r(`${io}Property`).toString());
	return {
		type: u,
		timeout: d,
		propCount: f,
		hasTransform: p
	};
}
function So(e, t) {
	for (; e.length < t.length;) e = e.concat(e);
	return Math.max(...t.map((t, n) => Co(t) + Co(e[n])));
}
function Co(e) {
	return e === "auto" ? 0 : Number(e.slice(0, -1).replace(",", ".")) * 1e3;
}
function wo(e) {
	return (e ? e.ownerDocument : document).body.offsetHeight;
}
function To(e, t, n) {
	let r = e[oo];
	r && (t = (t ? [t, ...r] : [...r]).join(" ")), t == null ? e.removeAttribute("class") : n ? e.setAttribute("class", t) : e.className = t;
}
var Eo = /* @__PURE__ */ Symbol("_vod"), Do = /* @__PURE__ */ Symbol("_vsh"), Oo = {
	name: "show",
	beforeMount(e, { value: t }, { transition: n }) {
		e[Eo] = e.style.display === "none" ? "" : e.style.display, n && t ? n.beforeEnter(e) : ko(e, t);
	},
	mounted(e, { value: t }, { transition: n }) {
		n && t && n.enter(e);
	},
	updated(e, { value: t, oldValue: n }, { transition: r }) {
		!t != !n && (r ? t ? (r.beforeEnter(e), ko(e, !0), r.enter(e)) : r.leave(e, () => {
			ko(e, !1);
		}) : ko(e, t));
	},
	beforeUnmount(e, { value: t }) {
		ko(e, t);
	}
};
function ko(e, t) {
	e.style.display = t ? e[Eo] : "none", e[Do] = !t;
}
var Ao = /* @__PURE__ */ Symbol(""), jo = /(?:^|;)\s*display\s*:/;
function Mo(e, t, n) {
	let r = e.style, i = _(n), a = !1;
	if (n && !i) {
		if (t) if (_(t)) for (let e of t.split(";")) {
			let t = e.slice(0, e.indexOf(":")).trim();
			n[t] ?? Po(r, t, "");
		}
		else for (let e in t) n[e] ?? Po(r, e, "");
		for (let i in n) {
			i === "display" && (a = !0);
			let o = n[i];
			o == null ? Po(r, i, "") : Ro(e, i, !_(t) && t ? t[i] : void 0, o) || Po(r, i, o);
		}
	} else if (i) {
		if (t !== n) {
			let e = r[Ao];
			e && (n += ";" + e), r.cssText = n, a = jo.test(n);
		}
	} else t && e.removeAttribute("style");
	Eo in e && (e[Eo] = a ? r.display : "", e[Do] && (r.display = "none"));
}
var No = /\s*!important$/;
function Po(e, t, n) {
	if (d(n)) n.forEach((n) => Po(e, t, n));
	else if (n ??= "", t.startsWith("--")) e.setProperty(t, n);
	else {
		let r = Lo(e, t);
		No.test(n) ? e.setProperty(j(r), n.replace(No, ""), "important") : e[r] = n;
	}
}
var Fo = [
	"Webkit",
	"Moz",
	"ms"
], Io = {};
function Lo(e, t) {
	let n = Io[t];
	if (n) return n;
	let r = k(t);
	if (r !== "filter" && r in e) return Io[t] = r;
	r = M(r);
	for (let n = 0; n < Fo.length; n++) {
		let i = Fo[n] + r;
		if (i in e) return Io[t] = i;
	}
	return t;
}
function Ro(e, t, n, r) {
	return e.tagName === "TEXTAREA" && (t === "width" || t === "height") && _(r) && n === r;
}
var zo = "http://www.w3.org/1999/xlink";
function Bo(e, t, n, r, i, a = se(t)) {
	r && t.startsWith("xlink:") ? n == null ? e.removeAttributeNS(zo, t.slice(6, t.length)) : e.setAttributeNS(zo, t, n) : n == null || a && !ce(n) ? e.removeAttribute(t) : e.setAttribute(t, a ? "" : v(n) ? String(n) : n);
}
function Vo(e, t, n, r, i) {
	if (t === "innerHTML" || t === "textContent") {
		n != null && (e[t] = t === "innerHTML" ? Qa(n) : n);
		return;
	}
	let a = e.tagName;
	if (t === "value" && a !== "PROGRESS" && !a.includes("-")) {
		let r = a === "OPTION" ? e.getAttribute("value") || "" : e.value, i = n == null ? e.type === "checkbox" ? "on" : "" : String(n);
		(r !== i || !("_value" in e)) && (e.value = i), n ?? e.removeAttribute(t), e._value = n;
		return;
	}
	let o = !1;
	if (n === "" || n == null) {
		let r = typeof e[t];
		r === "boolean" ? n = ce(n) : n == null && r === "string" ? (n = "", o = !0) : r === "number" && (n = 0, o = !0);
	}
	try {
		e[t] = n;
	} catch {}
	o && e.removeAttribute(i || t);
}
function Ho(e, t, n, r) {
	e.addEventListener(t, n, r);
}
function Uo(e, t, n, r) {
	e.removeEventListener(t, n, r);
}
var Wo = /* @__PURE__ */ Symbol("_vei");
function Go(e, t, n, r, i = null) {
	let a = e[Wo] || (e[Wo] = {}), o = a[t];
	if (r && o) o.value = r;
	else {
		let [n, s] = Jo(t);
		r ? Ho(e, n, a[t] = Qo(r, i), s) : o && (Uo(e, n, o, s), a[t] = void 0);
	}
}
var Ko = /(Once|Passive|Capture)$/, qo = /^on:?(?:Once|Passive|Capture)$/;
function Jo(e) {
	let t, n;
	for (; (n = e.match(Ko)) && !qo.test(e);) t ||= {}, e = e.slice(0, e.length - n[1].length), t[n[1].toLowerCase()] = !0;
	return [e[2] === ":" ? e.slice(3) : j(e.slice(2)), t];
}
var Yo = 0, Xo = /* @__PURE__ */ Promise.resolve(), Zo = () => Yo ||= (Xo.then(() => Yo = 0), Date.now());
function Qo(e, t) {
	let n = (e) => {
		if (!e._vts) e._vts = Date.now();
		else if (e._vts <= n.attached) return;
		let r = n.value;
		if (d(r)) {
			let n = e.stopImmediatePropagation;
			e.stopImmediatePropagation = () => {
				n.call(e), e._stopped = !0;
			};
			let i = r.slice(), a = [e];
			for (let n = 0; n < i.length && !e._stopped; n++) {
				let e = i[n];
				e && $t(e, t, 5, a);
			}
		} else $t(r, t, 5, [e]);
	};
	return n.value = e, n.attached = Zo(), n;
}
var $o = (e) => e.charCodeAt(0) === 111 && e.charCodeAt(1) === 110 && e.charCodeAt(2) > 96 && e.charCodeAt(2) < 123, es = (e, t, n, r, i, s) => {
	let c = i === "svg";
	t === "class" ? To(e, r, c) : t === "style" ? Mo(e, n, r) : a(t) ? o(t) || Go(e, t, n, r, s) : (t[0] === "." ? (t = t.slice(1), !0) : t[0] === "^" ? (t = t.slice(1), !1) : ts(e, t, r, c)) ? (Vo(e, t, r), !e.tagName.includes("-") && (t === "value" || t === "checked" || t === "selected") && Bo(e, t, r, c, s, t !== "value")) : e._isVueCE && (ns(e, t) || e._def.__asyncLoader && (/[A-Z]/.test(t) || !_(r))) ? Vo(e, k(t), r, s, t) : (t === "true-value" ? e._trueValue = r : t === "false-value" && (e._falseValue = r), Bo(e, t, r, c));
};
function ts(e, t, n, r) {
	if (r) return !!(t === "innerHTML" || t === "textContent" || t in e && $o(t) && g(n));
	if (t === "spellcheck" || t === "draggable" || t === "translate" || t === "autocorrect" || t === "sandbox" && e.tagName === "IFRAME" || t === "form" || t === "list" && e.tagName === "INPUT" || t === "type" && e.tagName === "TEXTAREA") return !1;
	if (t === "width" || t === "height") {
		let t = e.tagName;
		if (t === "IMG" || t === "VIDEO" || t === "CANVAS" || t === "SOURCE") return !1;
	}
	return $o(t) && _(n) ? !1 : t in e;
}
function ns(e, t) {
	let n = e._def.props;
	if (!n) return !1;
	let r = k(t);
	return Array.isArray(n) ? n.some((e) => k(e) === r) : Object.keys(n).some((e) => k(e) === r);
}
var rs = (e) => {
	let t = e.props["onUpdate:modelValue"] || !1;
	return d(t) ? (e) => F(t, e) : t;
};
function is(e) {
	e.target.composing = !0;
}
function as(e) {
	let t = e.target;
	t.composing && (t.composing = !1, t.dispatchEvent(new Event("input")));
}
var os = /* @__PURE__ */ Symbol("_assign");
function ss(e, t, n) {
	return t && (e = e.trim()), n && (e = ee(e)), e;
}
var $ = {
	created(e, { modifiers: { lazy: t, trim: n, number: r } }, i) {
		e[os] = rs(i);
		let a = r || i.props && i.props.type === "number";
		Ho(e, t ? "change" : "input", (t) => {
			t.target.composing || e[os](ss(e.value, n, a));
		}), (n || a) && Ho(e, "change", () => {
			e.value = ss(e.value, n, a);
		}), t || (Ho(e, "compositionstart", is), Ho(e, "compositionend", as), Ho(e, "change", as));
	},
	mounted(e, { value: t }) {
		e.value = t ?? "";
	},
	beforeUpdate(e, { value: t, oldValue: n, modifiers: { lazy: r, trim: i, number: a } }, o) {
		if (e[os] = rs(o), e.composing) return;
		let s = (a || e.type === "number") && !/^0\d/.test(e.value) ? ee(e.value) : e.value, c = t ?? "";
		if (s === c) return;
		let l = e.getRootNode();
		(l instanceof Document || l instanceof ShadowRoot) && l.activeElement === e && e.type !== "range" && (r && t === n || i && e.value.trim() === c) || (e.value = c);
	}
}, cs = {
	deep: !0,
	created(e, t, n) {
		e[os] = rs(n), Ho(e, "change", () => {
			let t = e._modelValue, n = ps(e), r = e.checked, i = e[os];
			if (d(t)) {
				let e = de(t, n), a = e !== -1;
				if (r && !a) i(t.concat(n));
				else if (!r && a) {
					let n = [...t];
					n.splice(e, 1), i(n);
				}
			} else if (p(t)) {
				let e = new Set(t);
				r ? e.add(n) : e.delete(n), i(e);
			} else i(ms(e, r));
		});
	},
	mounted: ls,
	beforeUpdate(e, t, n) {
		e[os] = rs(n), ls(e, t, n);
	}
};
function ls(e, { value: t, oldValue: n }, r) {
	e._modelValue = t;
	let i;
	if (d(t)) i = de(t, r.props.value) > -1;
	else if (p(t)) i = t.has(r.props.value);
	else {
		if (t === n) return;
		i = ue(t, ms(e, !0));
	}
	e.checked !== i && (e.checked = i);
}
var us = {
	created(e, { value: t }, n) {
		e.checked = ue(t, n.props.value), e[os] = rs(n), Ho(e, "change", () => {
			e[os](ps(e));
		});
	},
	beforeUpdate(e, { value: t, oldValue: n }, r) {
		e[os] = rs(r), t !== n && (e.checked = ue(t, r.props.value));
	}
}, ds = {
	deep: !0,
	created(e, { value: t, modifiers: { number: n } }, r) {
		e._modelValue = t, Ho(e, "change", () => {
			let t = Array.prototype.filter.call(e.options, (e) => e.selected).map((e) => n ? ee(ps(e)) : ps(e));
			e[os](e.multiple ? p(e._modelValue) ? new Set(t) : t : t[0]), e._assigning = !0, un(() => {
				e._assigning = !1;
			});
		}), e[os] = rs(r);
	},
	mounted(e, { value: t }) {
		fs(e, t);
	},
	beforeUpdate(e, { value: t }, n) {
		e._modelValue = t, e[os] = rs(n);
	},
	updated(e, { value: t }) {
		e._assigning || fs(e, t);
	}
};
function fs(e, t) {
	let n = e.multiple, r = d(t);
	if (!(n && !r && !p(t))) {
		for (let i = 0, a = e.options.length; i < a; i++) {
			let a = e.options[i], o = ps(a);
			if (n) if (r) {
				let e = typeof o;
				a.selected = e === "string" || e === "number" ? t.some((e) => String(e) === String(o)) : de(t, o) > -1;
			} else a.selected = t.has(o);
			else if (ue(ps(a), t)) {
				e.selectedIndex !== i && (e.selectedIndex = i);
				return;
			}
		}
		!n && e.selectedIndex !== -1 && (e.selectedIndex = -1);
	}
}
function ps(e) {
	return "_value" in e ? e._value : e.value;
}
function ms(e, t) {
	let n = t ? "_trueValue" : "_falseValue";
	return n in e ? e[n] : t;
}
var hs = {
	created(e, t, n) {
		_s(e, t, n, null, "created");
	},
	mounted(e, t, n) {
		_s(e, t, n, null, "mounted");
	},
	beforeUpdate(e, t, n, r) {
		_s(e, t, n, r, "beforeUpdate");
	},
	updated(e, t, n, r) {
		_s(e, t, n, r, "updated");
	}
};
function gs(e, t) {
	switch (e) {
		case "SELECT": return ds;
		case "TEXTAREA": return $;
		default: switch (t) {
			case "checkbox": return cs;
			case "radio": return us;
			default: return $;
		}
	}
}
function _s(e, t, n, r, i) {
	let a = gs(e.tagName, n.props && n.props.type)[i];
	a && a(e, t, n, r);
}
var vs = [
	"ctrl",
	"shift",
	"alt",
	"meta"
], ys = {
	stop: (e) => e.stopPropagation(),
	prevent: (e) => e.preventDefault(),
	self: (e) => e.target !== e.currentTarget,
	ctrl: (e) => !e.ctrlKey,
	shift: (e) => !e.shiftKey,
	alt: (e) => !e.altKey,
	meta: (e) => !e.metaKey,
	left: (e) => "button" in e && e.button !== 0,
	middle: (e) => "button" in e && e.button !== 1,
	right: (e) => "button" in e && e.button !== 2,
	exact: (e, t) => vs.some((n) => e[`${n}Key`] && !t.includes(n))
}, bs = (e, t) => {
	if (!e) return e;
	let n = e._withMods ||= {}, r = t.join(".");
	return n[r] || (n[r] = ((n, ...r) => {
		for (let e = 0; e < t.length; e++) {
			let r = ys[t[e]];
			if (r && r(n, t)) return;
		}
		return e(n, ...r);
	}));
}, xs = {
	esc: "escape",
	space: " ",
	up: "arrow-up",
	left: "arrow-left",
	right: "arrow-right",
	down: "arrow-down",
	delete: "backspace"
}, Ss = (e, t) => {
	let n = e._withKeys ||= {}, r = t.join(".");
	return n[r] || (n[r] = ((n) => {
		if (!("key" in n)) return;
		let r = j(n.key);
		if (t.some((e) => e === r || xs[e] === r)) return e(n);
	}));
}, Cs = /* @__PURE__ */ s({ patchProp: es }, ro), ws;
function Ts() {
	return ws ||= Wi(Cs);
}
var Es = ((...e) => {
	let t = Ts().createApp(...e), { mount: n } = t;
	return t.mount = (e) => {
		let r = Os(e);
		if (!r) return;
		let i = t._component;
		!g(i) && !i.render && !i.template && (i.template = r.innerHTML), r.nodeType === 1 && (r.textContent = "");
		let a = n(r, !1, Ds(r));
		return r instanceof Element && (r.removeAttribute("v-cloak"), r.setAttribute("data-v-app", "")), a;
	}, t;
});
function Ds(e) {
	if (e instanceof SVGElement) return "svg";
	if (typeof MathMLElement == "function" && e instanceof MathMLElement) return "mathml";
}
function Os(e) {
	return _(e) ? document.querySelector(e) : e;
}
//#endregion
//#region src/shared/api.ts
async function ks(e) {
	if (e.ok) return await e.json();
	let t = e.statusText || "Request failed";
	try {
		let n = await e.json();
		if (typeof n.detail == "string") t = n.detail;
		else if (typeof n.message == "string") t = n.message;
		else if (n.detail && typeof n.detail == "object") {
			let e = n.detail;
			t = e.message || e.detail || t;
		}
	} catch {}
	throw Error(t);
}
async function As(e) {
	return ks(await fetch(e, {
		credentials: "same-origin",
		headers: { Accept: "application/json" }
	}));
}
async function js(e, t = {}) {
	return ks(await fetch(e, {
		method: "POST",
		credentials: "same-origin",
		headers: {
			Accept: "application/json",
			"Content-Type": "application/json"
		},
		body: JSON.stringify(t)
	}));
}
//#endregion
//#region src/chat/markdown.ts
function Ms(e) {
	return String(e ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll("\"", "&quot;").replaceAll("'", "&#039;");
}
function Ns(e) {
	let t = String(e ?? "").trim(), n = t.toLowerCase();
	return n.startsWith("https://") || n.startsWith("http://") || n.startsWith("mailto:") || n.startsWith("tel:") || t.startsWith("/") || t.startsWith("#") ? t : "";
}
function Ps(e) {
	let t = [], n = String(e ?? "");
	return n = n.replace(/`([^`\n]+)`/g, (e, n) => {
		let r = `@@TC_${t.length}@@`;
		return t.push(`<code>${Ms(n)}</code>`), r;
	}), n = n.replace(/\[([^\]\n]+)\]\(([^)\s]+)\)/g, (e, n, r) => {
		let i = `@@TC_${t.length}@@`, a = Ns(r);
		return t.push(a ? `<a href="${Ms(a)}" target="_blank" rel="noopener noreferrer">${Ms(n)}</a>` : `${Ms(n)} (${Ms(r)})`), i;
	}), n = Ms(n).replace(/\*\*([^*\n][^*\n]*?)\*\*/g, "<strong>$1</strong>").replace(/\*([^*\n][^*\n]*?)\*/g, "<em>$1</em>"), t.forEach((e, t) => {
		n = n.replaceAll(`@@TC_${t}@@`, e);
	}), n;
}
function Fs(e) {
	let t = String(e ?? "").replace(/\r\n?/g, "\n").split("\n"), n = [], r = [], i = "", a = [], o = !1, s = "", c = [], l = () => {
		r.length && (n.push(`<p>${r.map((e) => Ps(e.trim())).join("<br />")}</p>`), r = []);
	}, u = () => {
		i && a.length && n.push(`<${i}>${a.map((e) => `<li>${Ps(e)}</li>`).join("")}</${i}>`), i = "", a = [];
	}, d = () => {
		if (!o) return;
		let e = s.replace(/[^A-Za-z0-9_+\-]/g, "");
		n.push(`<pre><code${e ? ` class="language-${e}"` : ""}>${Ms(c.join("\n"))}</code></pre>`), o = !1, s = "", c = [];
	};
	return t.forEach((e) => {
		let t = e.match(/^```(?:\s*([A-Za-z0-9_+\-]+))?\s*$/);
		if (t) {
			l(), u(), o ? d() : (o = !0, s = String(t[1] || ""));
			return;
		}
		if (o) {
			c.push(e);
			return;
		}
		let f = e.trim();
		if (!f) {
			l(), u();
			return;
		}
		let p = f.match(/^(#{1,6})\s+(.*)$/);
		if (p) {
			l(), u();
			let e = Math.min(6, p[1].length);
			n.push(`<h${e}>${Ps(p[2])}</h${e}>`);
			return;
		}
		let m = f.match(/^\d+\.\s+(.*)$/), h = f.match(/^[-*+]\s+(.*)$/);
		if (m || h) {
			l();
			let e = m ? "ol" : "ul";
			i && i !== e && u(), i = e, a.push(String((m || h)?.[1] || ""));
			return;
		}
		if (f.startsWith("> ")) {
			l(), u(), n.push(`<blockquote>${Ps(f.slice(2))}</blockquote>`);
			return;
		}
		i && u(), r.push(f);
	}), l(), u(), d(), n.join("") || `<p>${Ps(e)}</p>`;
}
//#endregion
//#region src/chat/components/ChatMessage.vue?vue&type=script&setup=true&lang.ts
var Is = {
	key: 0,
	class: "chat-avatar"
}, Ls = ["src", "alt"], Rs = {
	key: 1,
	class: "chat-avatar-fallback assistant"
}, zs = { class: "role" }, Bs = ["aria-label"], Vs = { class: "chat-typing-label" }, Hs = {
	key: 1,
	class: "bubble-body"
}, Us = ["innerHTML"], Ws = {
	key: 3,
	class: "bubble-body"
}, Gs = ["src", "alt"], Ks = {
	key: 5,
	class: "chat-media-wrap"
}, qs = ["src"], Js = { class: "chat-file-meta" }, Ys = ["href", "download"], Xs = {
	key: 6,
	class: "chat-media-wrap"
}, Zs = ["src"], Qs = { class: "chat-file-meta" }, $s = ["href", "download"], ec = {
	key: 7,
	class: "chat-file-card"
}, tc = { class: "chat-file-meta" }, nc = ["href", "download"], rc = { key: 8 }, ic = {
	key: 1,
	class: "chat-avatar"
}, ac = ["src", "alt"], oc = {
	key: 1,
	class: "chat-avatar-fallback user"
}, sc = /* @__PURE__ */ sr({
	__name: "ChatMessage",
	props: {
		message: {},
		profile: {},
		filesEndpoint: {}
	},
	emits: ["mediaReady"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = Q(() => String(n.message.role || "assistant").toLowerCase() === "user" ? "user" : "assistant"), a = Q(() => i.value === "user"), o = Q(() => {
			let e = String(n.profile.tater_first_name || n.profile.tater_name || "Tater").trim() || "Tater", t = String(n.profile.tater_last_name || "Totterson").trim();
			return String(n.profile.tater_full_name || [e, t].filter(Boolean).join(" ") || "Tater Totterson").trim();
		}), s = Q(() => a.value ? String(n.message.username || n.profile.username || "User") : o.value), c = Q(() => String(a.value ? n.profile.user_avatar || "" : n.profile.tater_avatar || "")), l = Q(() => (s.value.match(/[A-Za-z0-9]/)?.[0] || (a.value ? "U" : "T")).toUpperCase()), u = Q(() => {
			let e = n.message.content;
			return e && typeof e == "object" ? e : null;
		}), d = Q(() => String(u.value?.marker || "").trim().toLowerCase()), f = Q(() => String(u.value?.type || "").trim().toLowerCase()), p = Q(() => String(u.value?.name || "attachment").trim() || "attachment"), m = Q(() => {
			let e = String(u.value?.mimetype || "application/octet-stream").trim();
			return /^[A-Za-z0-9.+-]+\/[A-Za-z0-9.+-]+$/.test(e) ? e : "application/octet-stream";
		}), h = Q(() => {
			let e = Number(u.value?.size || 0);
			return !Number.isFinite(e) || e <= 0 ? "" : e < 1024 ? `${Math.round(e)} B` : e < 1024 ** 2 ? `${(e / 1024).toFixed(1)} KB` : e < 1024 ** 3 ? `${(e / 1024 ** 2).toFixed(1)} MB` : `${(e / 1024 ** 3).toFixed(1)} GB`;
		}), g = Q(() => {
			let e = u.value;
			if (!e) return "";
			let t = String(e.data_b64 || "").trim();
			if (t) return `data:${m.value};base64,${t}`;
			let r = String(e.url || e.src || e.href || "").trim();
			if (r) {
				let e = Ns(r);
				if (!e || /^(mailto:|tel:|#)/i.test(e)) return "";
				if (e.startsWith("/")) {
					let t = n.filesEndpoint.indexOf("/api/chat/files");
					return `${t >= 0 ? n.filesEndpoint.slice(0, t) : ""}${e}`;
				}
				return e;
			}
			let i = String(e.id || e.file_id || "").trim();
			return i ? `${n.filesEndpoint}/${encodeURIComponent(i)}?mimetype=${encodeURIComponent(m.value)}` : "";
		}), _ = Q(() => typeof n.message.content == "string" ? n.message.content : ""), v = Q(() => String(u.value?.content || "Working on it…")), y = Q(() => {
			try {
				return JSON.stringify(u.value, null, 2);
			} catch {
				return String(u.value ?? "");
			}
		});
		return (t, n) => (q(), J("article", { class: B(["chat-row", [i.value, { "typing-indicator": d.value === "typing" }]]) }, [
			a.value ? Z("", !0) : (q(), J("div", Is, [c.value ? (q(), J("img", {
				key: 0,
				class: "chat-avatar-img",
				src: c.value,
				alt: `${s.value} avatar`
			}, null, 8, Ls)) : (q(), J("div", Rs, V(l.value), 1))])),
			Y("div", { class: B(["bubble", i.value]) }, [Y("div", zs, V(s.value), 1), d.value === "typing" ? (q(), J("div", {
				key: 0,
				class: "bubble-body chat-typing-body",
				"aria-label": `${s.value} is typing`
			}, [Y("span", Vs, V(s.value) + " is typing", 1), n[6] ||= Y("span", {
				class: "chat-typing-dots",
				"aria-hidden": "true"
			}, [
				Y("span"),
				Y("span"),
				Y("span")
			], -1)], 8, Bs)) : d.value === "plugin_wait" ? (q(), J("div", Hs, V(v.value), 1)) : typeof e.message.content == "string" && !a.value ? (q(), J("div", {
				key: 2,
				class: "bubble-body markdown",
				innerHTML: Vt(Fs)(_.value)
			}, null, 8, Us)) : typeof e.message.content == "string" ? (q(), J("div", Ws, V(_.value), 1)) : f.value === "image" && g.value ? (q(), J("img", {
				key: 4,
				class: "chat-media-image",
				src: g.value,
				alt: p.value,
				onLoad: n[0] ||= (e) => r("mediaReady"),
				onError: n[1] ||= (e) => r("mediaReady")
			}, null, 40, Gs)) : f.value === "audio" && g.value ? (q(), J("div", Ks, [
				Y("audio", {
					controls: "",
					preload: "metadata",
					src: g.value,
					onLoadedmetadata: n[2] ||= (e) => r("mediaReady"),
					onError: n[3] ||= (e) => r("mediaReady")
				}, null, 40, qs),
				Y("div", Js, V(p.value), 1),
				Y("a", {
					class: "tv-button tc-download",
					href: g.value,
					download: p.value
				}, "Download audio", 8, Ys)
			])) : f.value === "video" && g.value ? (q(), J("div", Xs, [
				Y("video", {
					controls: "",
					preload: "metadata",
					src: g.value,
					class: "chat-media-video",
					onLoadedmetadata: n[4] ||= (e) => r("mediaReady"),
					onError: n[5] ||= (e) => r("mediaReady")
				}, null, 40, Zs),
				Y("div", Qs, V(p.value), 1),
				Y("a", {
					class: "tv-button tc-download",
					href: g.value,
					download: p.value
				}, "Download video", 8, $s)
			])) : f.value === "file" && g.value ? (q(), J("div", ec, [Y("div", tc, [X(V(p.value), 1), h.value ? (q(), J(K, { key: 0 }, [X(" (" + V(h.value) + ")", 1)], 64)) : Z("", !0)]), Y("a", {
				class: "tv-button tc-download",
				href: g.value,
				download: p.value
			}, "Download file", 8, nc)])) : (q(), J("pre", rc, V(y.value), 1))], 2),
			a.value ? (q(), J("div", ic, [c.value ? (q(), J("img", {
				key: 0,
				class: "chat-avatar-img",
				src: c.value,
				alt: `${s.value} avatar`
			}, null, 8, ac)) : (q(), J("div", oc, V(l.value), 1))])) : Z("", !0)
		], 2));
	}
}), cc = { class: "tater-vue-surface tc-chat" }, lc = { class: "tv-panel chat-feed-card tc-feed-card" }, uc = {
	key: 0,
	class: "tc-empty-chat"
}, dc = { class: "tc-empty-avatar" }, fc = ["data-chat-stream-job"], pc = {
	key: 0,
	class: "tc-attachment-tray"
}, mc = { class: "tc-attachment-list" }, hc = ["title", "onClick"], gc = {
	key: 1,
	class: "tc-job-strip",
	"aria-label": "Active chat jobs"
}, _c = { key: 0 }, vc = { class: "message-box chat-composer-card tc-composer-card" }, yc = {
	class: "chat-composer",
	role: "group",
	"aria-label": "Chat composer"
}, bc = { class: "chat-composer-bar" }, xc = {
	class: "chat-composer-btn chat-composer-attach",
	title: "Attach files",
	"aria-label": "Attach files"
}, Sc = ["placeholder"], Cc = [
	"disabled",
	"title",
	"aria-label"
], wc = {
	key: 2,
	class: "chat-speed-stats tc-speed-stats"
}, Tc = {
	class: "chat-live-status tc-live-status",
	"aria-live": "polite"
}, Ec = 32, Dc = /* @__PURE__ */ sr({
	__name: "ChatApp",
	props: {
		state: {},
		options: {}
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ U(null), r = /* @__PURE__ */ U(null), i = /* @__PURE__ */ U(null), a = /* @__PURE__ */ U(""), o = /* @__PURE__ */ U([]), s = /* @__PURE__ */ U(!1), c = /* @__PURE__ */ U(""), l = /* @__PURE__ */ U(String(t.options.sessionId || "")), u = /* @__PURE__ */ U([]), d = /* @__PURE__ */ U({}), f = /* @__PURE__ */ U({ ...t.options.initialJobs || {} }), p = /* @__PURE__ */ U(!0), m = {}, h = {}, g = {}, _ = {}, v = {}, y = Q(() => t.state.profile || {}), b = Q(() => Array.isArray(t.state.messages) ? t.state.messages : []), x = Q(() => {
			let e = String(y.value.tater_first_name || y.value.tater_name || "Tater").trim() || "Tater", t = String(y.value.tater_last_name || "Totterson").trim();
			return String(y.value.tater_full_name || [e, t].filter(Boolean).join(" ") || "Tater Totterson").trim();
		}), S = Q(() => Object.entries(f.value).filter(([, e]) => !!e)), C = Q(() => S.value.length), w = Q(() => Object.entries(d.value).filter(([, e]) => !!e)), T = Q(() => ({
			role: "assistant",
			content: { marker: "typing" }
		})), E = Q(() => {
			if (!C.value) return c.value;
			let e = /* @__PURE__ */ new Map();
			S.value.forEach(([, t]) => {
				let n = String(t.current_tool || "").trim();
				if (!n) return;
				let r = n.toLowerCase(), i = e.get(r) || {
					label: n,
					count: 0
				};
				i.count += 1, e.set(r, i);
			});
			let t = [...e.values()].sort((e, t) => t.count - e.count).slice(0, 3).map((e) => `${e.count} using ${e.label}`);
			return `${C.value} ${C.value === 1 ? "job" : "jobs"} running${t.length ? ` • ${t.join(" • ")}` : ""}`;
		}), D = Q(() => {
			if (!t.state.stats?.enabled) return "";
			let e = t.state.stats.stats;
			if (!e || typeof e != "object") return "";
			let n = Number(e.elapsed || 0), r = Number(e.total_tokens || 0), i = Number(e.tps_total || 0), a = Number(e.tps_prompt || 0), o = Number(e.tps_comp || 0), s = o > 0 ? o : i;
			if (!(n > 0 && r > 0 && s > 0)) return "";
			let c = String(e.speed_basis || ""), l = ["llama_cpp_timing", "mlx_lm_timing"].includes(c) ? "decode" : c === "local_generate" ? "generated" : c === "api_round_trip" ? "API completion" : "completion", u = [];
			i > 0 && Math.abs(i - s) >= 1 && u.push(`total ${Math.round(i)} tok/s`), a > 0 && u.push(`prompt ${Math.round(a)} tok/s`);
			let d = Number(e.prompt_tokens || 0), f = Number(e.completion_tokens || 0);
			return `${String(e.model || "LLM")} — ${l}: ${Math.round(s)} tok/s${u.length ? ` · ${u.join(" · ")}` : ""} • ${Math.round(r)} tok in ${n.toFixed(2)}s (prompt ${Math.round(d)}, generated ${Math.round(f)})`;
		});
		function O(e, n = "success") {
			t.options.onToast?.(e, n);
		}
		function k(e, n) {
			let r = e instanceof Error ? e.message : n;
			return c.value = r, t.options.onRequestError?.(r), r;
		}
		function A() {
			t.options.onJobsChange?.({ ...f.value });
		}
		function j(e) {
			m[e]?.close(), delete m[e];
		}
		function M(e) {
			h[e] && window.clearTimeout(h[e]), delete h[e];
		}
		function N(e) {
			_[e] && window.clearTimeout(_[e]), delete _[e], delete v[e], delete g[e];
		}
		function P(e, t) {
			let n = Math.max(0, e.length - t);
			if (!n) return t;
			let r = n > 240 ? 48 : n > 120 ? 28 : n > 48 ? 18 : 10, i = Math.min(e.length, t + r);
			if (i >= e.length || /\s/.test(e.charAt(i - 1))) return i;
			let a = e.slice(i, Math.min(e.length, i + 14)).search(/\s/);
			return a >= 0 && (i += a + 1), Math.min(e.length, i);
		}
		function F(e) {
			delete _[e];
			let t = String(g[e] || ""), n = String(d.value[e] || "");
			if (!t || t === n) return;
			let r = t.startsWith(n) ? t.slice(0, P(t, n.length)) : t;
			d.value = {
				...d.value,
				[e]: r
			}, v[e] = performance.now(), r.length < t.length && I(e);
		}
		function I(e) {
			if (_[e]) return;
			let t = performance.now() - Number(v[e] || 0), n = Math.max(0, Ec - t);
			_[e] = window.setTimeout(() => F(e), n);
		}
		function ee(e, t) {
			g[e] = String(g[e] || d.value[e] || "") + t, I(e);
		}
		function te(e) {
			_[e] && window.clearTimeout(_[e]), delete _[e];
			let t = String(g[e] || "");
			t && d.value[e] !== t && (d.value = {
				...d.value,
				[e]: t
			});
		}
		function ne(e, t) {
			let n = f.value[e] || {};
			f.value = {
				...f.value,
				[e]: {
					...n,
					...t,
					status: String(t.status || n.status || "running").toLowerCase(),
					updated_at: Date.now()
				}
			}, A();
		}
		function L(e) {
			let t = { ...f.value };
			delete t[e], f.value = t, A();
		}
		function R() {
			p.value = !0, un(() => {
				n.value && (n.value.scrollTop = n.value.scrollHeight);
			});
		}
		function z() {
			p.value && R();
		}
		function re() {
			let e = n.value;
			e && (p.value = e.scrollHeight - e.scrollTop - e.clientHeight < 120);
		}
		async function ie() {
			let e = await As(t.options.endpoints.history);
			t.state.messages = Array.isArray(e.messages) ? e.messages : [], u.value = [];
		}
		async function ae() {
			try {
				t.state.stats = await As(t.options.endpoints.stats);
			} catch {
				t.state.stats = {
					enabled: !1,
					stats: null
				};
			}
		}
		async function B(e, n, r = []) {
			if (!f.value[e]) return;
			j(e), M(e), te(e);
			try {
				await ie();
			} catch (e) {
				r.length ? t.state.messages = [...t.state.messages, ...r.map((e) => ({
					role: "assistant",
					username: "assistant",
					content: e
				}))] : k(e, "Chat history refresh failed.");
			}
			L(e), N(e);
			let i = { ...d.value };
			delete i[e], d.value = i, await ae(), t.options.onHealthRefresh?.(), c.value = n, R();
		}
		function oe(e, t) {
			let n = String(t.status || "running").trim().toLowerCase();
			if (n === "done") {
				B(e, "Complete.", Array.isArray(t.responses) ? t.responses : []);
				return;
			}
			if (n === "error") {
				B(e, `Job failed: ${String(t.error || "unknown error")}`);
				return;
			}
			ne(e, {
				status: n || "running",
				current_tool: String(t.current_tool || "").trim(),
				task_name: String(t.task_name || f.value[e]?.task_name || "").trim()
			});
		}
		function se(e, n) {
			M(e), f.value[e] && (h[e] = window.setTimeout(async () => {
				if (f.value[e]) {
					try {
						oe(e, await As(`${t.options.endpoints.jobs}/${encodeURIComponent(e)}`));
					} catch (e) {
						t.options.onRequestError?.(e instanceof Error ? e.message : "Chat job polling failed.");
					}
					f.value[e] && se(e, 1200);
				}
			}, Math.max(250, n ?? (t.options.isIngress ? 900 : 2e3))));
		}
		function ce(e) {
			try {
				return JSON.parse(String(e.data || "{}"));
			} catch {
				return {};
			}
		}
		function le(e, n = {}) {
			if (!e || (ne(e, {
				status: "queued",
				...n
			}), j(e), se(e), typeof EventSource != "function")) return;
			let r = new EventSource(`${t.options.endpoints.jobs}/${encodeURIComponent(e)}/events`);
			m[e] = r, r.addEventListener("status", (t) => oe(e, ce(t))), r.addEventListener("tool", (t) => {
				let n = ce(t);
				ne(e, {
					status: "running",
					current_tool: String(n.current_tool || "tool"),
					task_name: String(n.task_name || f.value[e]?.task_name || "")
				});
			}), r.addEventListener("waiting", (e) => {
				let t = String(ce(e).wait_text || "").trim();
				t && (u.value = [...u.value, {
					role: "assistant",
					content: {
						marker: "plugin_wait",
						content: t
					}
				}], z());
			}), r.addEventListener("response_chunk", (t) => {
				let n = String(ce(t).chunk || "");
				n && ee(e, n);
			}), r.addEventListener("done", (t) => {
				let n = ce(t);
				B(e, "Complete.", Array.isArray(n.responses) ? n.responses : []);
			}), r.addEventListener("job_error", (t) => {
				B(e, `Job failed: ${String(ce(t).error || "unknown error")}`);
			}), r.onerror = () => j(e);
		}
		function ue(e) {
			return e < 1024 ? `${e} B` : e < 1024 ** 2 ? `${(e / 1024).toFixed(1)} KB` : `${(e / 1024 ** 2).toFixed(1)} MB`;
		}
		function de(e) {
			let t = e.target, n = Array.from(t.files || []), r = Number(y.value.attach_max_mb_each || 0) * 1024 ** 2, i = Number(y.value.attach_max_mb_total || 0) * 1024 ** 2, a = [], s = 0;
			for (let e of n) {
				if (r > 0 && e.size > r) {
					O(`${e.name} is larger than the ${y.value.attach_max_mb_each} MB attachment limit.`, "error");
					continue;
				}
				if (i > 0 && s + e.size > i) {
					O(`Attachments exceed the ${y.value.attach_max_mb_total} MB total limit.`, "error");
					break;
				}
				a.push(e), s += e.size;
			}
			o.value = a, t.value = "";
		}
		function fe(e) {
			o.value = o.value.filter((t, n) => n !== e);
		}
		function pe() {
			o.value = [], i.value && (i.value.value = "");
		}
		function me(e) {
			return new Promise((t, n) => {
				let r = new FileReader();
				r.onload = () => t(String(r.result || "")), r.onerror = () => n(/* @__PURE__ */ Error(`Could not read ${e.name}.`)), r.readAsDataURL(e);
			});
		}
		async function he() {
			if (s.value) return;
			let e = a.value.trim(), n = [...o.value];
			if (!e && !n.length) {
				c.value = "Enter a message or attach files first.";
				return;
			}
			s.value = !0, a.value = "", pe(), _e(), c.value = n.length ? "Preparing attachments…" : "Queueing chat job…", R();
			try {
				let r = [];
				for (let e of n) r.push({
					name: e.name || "attachment",
					mimetype: e.type || "application/octet-stream",
					data_url: await me(e)
				});
				let i = await js(t.options.endpoints.jobs, {
					message: e,
					session_id: l.value,
					attachments: r
				}), a = String(i.session_id || "").trim();
				a && (l.value = a, t.options.onSessionChange?.(a));
				let o = String(i.job_id || "").trim();
				if (!o) throw Error("Backend did not return a job id.");
				await ie(), le(o, {
					status: "queued",
					task_name: String(i.task_name || "")
				}), c.value = i.task_name ? `Job queued: ${i.task_name}` : "Job queued…", t.options.onHealthRefresh?.(), R();
			} catch (e) {
				O(`Chat failed: ${k(e, "Chat failed.")}`, "error");
			} finally {
				s.value = !1;
			}
		}
		function ge(e) {
			e.key === "Enter" && !e.shiftKey && !e.ctrlKey && !e.altKey && !e.metaKey && !e.isComposing && (e.preventDefault(), he());
		}
		function _e() {
			un(() => {
				let e = r.value;
				e && (e.style.height = "auto", e.style.height = `${Math.min(Math.max(e.scrollHeight, 44), 180)}px`);
			});
		}
		return On([
			() => b.value.length,
			() => u.value.length,
			() => w.value.map(([e, t]) => `${e}:${t.length}`).join("|"),
			C
		], z), On(a, _e), Er(() => {
			Object.entries(f.value).forEach(([e, t]) => le(e, t)), R();
		}), kr(() => {
			Object.keys(m).forEach(j), Object.keys(h).forEach(M), [.../* @__PURE__ */ new Set([...Object.keys(g), ...Object.keys(_)])].forEach((e) => N(e));
		}), (t, c) => (q(), J("div", cc, [Y("section", lc, [
			Y("div", {
				ref_key: "feed",
				ref: n,
				class: "chat-log tc-chat-log",
				onScroll: re
			}, [
				!b.value.length && !u.value.length && !w.value.length ? (q(), J("div", uc, [
					Y("div", dc, V(x.value.charAt(0)), 1),
					Y("h2", null, "Talk to " + V(x.value), 1),
					c[1] ||= Y("p", null, "Ask a question, control your home, or attach something for Tater to inspect.", -1)
				])) : Z("", !0),
				(q(!0), J(K, null, G(b.value, (t, n) => (q(), da(sc, {
					key: t.id || `history-${n}`,
					message: t,
					profile: y.value,
					"files-endpoint": e.options.endpoints.files,
					onMediaReady: z
				}, null, 8, [
					"message",
					"profile",
					"files-endpoint"
				]))), 128)),
				(q(!0), J(K, null, G(u.value, (t, n) => (q(), da(sc, {
					key: `ephemeral-${n}`,
					message: t,
					profile: y.value,
					"files-endpoint": e.options.endpoints.files
				}, null, 8, [
					"message",
					"profile",
					"files-endpoint"
				]))), 128)),
				(q(!0), J(K, null, G(w.value, ([t, n]) => (q(), J("div", {
					key: t,
					"data-chat-stream-job": t,
					"aria-live": "polite",
					"aria-busy": "true"
				}, [ga(sc, {
					message: {
						role: "assistant",
						content: n
					},
					profile: y.value,
					"files-endpoint": e.options.endpoints.files
				}, null, 8, [
					"message",
					"profile",
					"files-endpoint"
				])], 8, fc))), 128)),
				C.value && !w.value.length ? (q(), da(sc, {
					key: 1,
					message: T.value,
					profile: y.value,
					"files-endpoint": e.options.endpoints.files
				}, null, 8, [
					"message",
					"profile",
					"files-endpoint"
				])) : Z("", !0)
			], 544),
			o.value.length ? (q(), J("div", pc, [Y("div", mc, [(q(!0), J(K, null, G(o.value, (e, t) => (q(), J("button", {
				key: `${e.name}-${e.size}-${t}`,
				type: "button",
				class: "tc-attachment-chip",
				title: `Remove ${e.name}`,
				onClick: (e) => fe(t)
			}, [
				Y("span", null, V(e.name), 1),
				Y("small", null, V(ue(e.size)), 1),
				c[2] ||= Y("b", { "aria-hidden": "true" }, "×", -1)
			], 8, hc))), 128))]), Y("button", {
				type: "button",
				class: "tc-clear-files",
				onClick: pe
			}, "Clear all")])) : Z("", !0),
			S.value.length ? (q(), J("div", gc, [(q(!0), J(K, null, G(S.value, ([e, t]) => (q(), J("span", { key: e }, [
				c[3] ||= Y("i", null, null, -1),
				X(V(t.task_name || "Tater is working"), 1),
				t.current_tool ? (q(), J("small", _c, V(t.current_tool), 1)) : Z("", !0)
			]))), 128))])) : Z("", !0),
			Y("div", vc, [Y("div", yc, [Y("div", bc, [
				Y("label", xc, [Y("input", {
					ref_key: "fileInput",
					ref: i,
					class: "tc-file-input",
					type: "file",
					multiple: "",
					onChange: de
				}, null, 544), c[4] ||= Y("span", {
					class: "chat-composer-icon chat-composer-plus",
					"aria-hidden": "true"
				}, "+", -1)]),
				W(Y("textarea", {
					ref_key: "composer",
					ref: r,
					"onUpdate:modelValue": c[0] ||= (e) => a.value = e,
					class: "chat-composer-input",
					rows: "1",
					placeholder: `Message ${x.value}…`,
					onKeydown: ge
				}, null, 40, Sc), [[$, a.value]]),
				Y("button", {
					type: "button",
					class: "chat-composer-send",
					disabled: s.value,
					title: s.value ? "Preparing message" : "Send message",
					"aria-label": s.value ? "Preparing message" : "Send message",
					onClick: he
				}, [...c[5] ||= [Y("span", {
					class: "chat-composer-icon chat-composer-send-arrow",
					"aria-hidden": "true"
				}, "➤", -1)]], 8, Cc)
			])])]),
			D.value ? (q(), J("div", wc, V(D.value), 1)) : Z("", !0),
			Y("div", Tc, V(E.value), 1)
		])]));
	}
});
//#endregion
//#region src/music/api.ts
async function Oc(e) {
	if (e.ok) return await e.json();
	let t = e.statusText || "Request failed";
	try {
		let n = await e.json();
		if (typeof n.detail == "string") t = n.detail;
		else if (n.detail && typeof n.detail == "object") {
			let e = n.detail;
			t = e.message || e.detail || t;
		}
	} catch {}
	throw Error(t);
}
async function kc(e) {
	return Oc(await fetch(e, {
		headers: { Accept: "application/json" },
		credentials: "same-origin"
	}));
}
async function Ac(e, t, n) {
	return Oc(await fetch(e, {
		method: "POST",
		credentials: "same-origin",
		headers: {
			Accept: "application/json",
			"Content-Type": "application/json"
		},
		body: JSON.stringify({
			action: t,
			payload: n
		})
	}));
}
//#endregion
//#region src/music/playerDisplay.ts
var jc = [
	"satellite",
	"stereo",
	"airplay",
	"sonos",
	"home",
	"player"
], Mc = {
	satellite: "Tater Native Sats",
	stereo: "Tater Stereo Pairs",
	airplay: "AirPlay Devices",
	sonos: "Sonos Players",
	home: "Home Assistant Players",
	player: "Other Players"
};
function Nc(e) {
	let t = String(e ?? "").toLowerCase();
	return t.startsWith("voice_core:stereo:") || t.startsWith("stereo:") ? "stereo" : t.startsWith("voice_core:") || t.startsWith("native:") ? "satellite" : t.startsWith("airplay:") ? "airplay" : t.startsWith("sonos:") ? "sonos" : t.startsWith("ha:") ? "home" : "player";
}
function Pc(e, t) {
	let n = /* @__PURE__ */ new Map();
	for (let r of e) {
		let e = Nc(t(r));
		n.set(e, [...n.get(e) || [], r]);
	}
	return jc.filter((e) => n.has(e)).map((e) => ({
		key: e,
		label: Mc[e],
		items: n.get(e) || []
	}));
}
function Fc(e) {
	return e.replace(/^(?:Tater\s+(?:Satellite|Sat|Stereo)|AirPlay(?:\s+Bridge)?|Sonos|Home\s+Assistant|Saved\s+player)\s*:\s*/i, "");
}
function Ic(e) {
	let t = String(e ?? "").trim(), n = "", r = t.match(/\s*•\s*(offline(?:\s+or\s+firmware\s+update\s+required)?|online)\s*$/i);
	r && (n = r[1].replace(/^./, (e) => e.toUpperCase()), t = t.slice(0, r.index).trim()), t = Fc(t);
	let i = "", a = t.lastIndexOf(" (");
	return a >= 0 && t.endsWith(")") && (i = t.slice(a + 2, -1).trim(), t = t.slice(0, a).trim()), {
		name: t || String(e ?? "").trim(),
		detail: i,
		status: n
	};
}
function Lc(e, t) {
	return Ic(e).name || String(t ?? "").trim() || "Unnamed player";
}
function Rc(e, t, n) {
	let r = Nc(n), i = Ic(e);
	if (r === "satellite") {
		let e = i.detail.split("•", 1)[0].trim();
		return [e && !/^(?:native|voice_core):/i.test(e) && e !== i.name ? e : "", i.status].filter(Boolean).join(" · ");
	}
	return r === "stereo" || r === "airplay" ? i.status : String(t ?? "").trim();
}
//#endregion
//#region src/music/components/DynamicField.vue?vue&type=script&setup=true&lang.ts
var zc = ["checked", "disabled"], Bc = { key: 0 }, Vc = {
	key: 0,
	class: "tm-choice-card-grid",
	role: "group"
}, Hc = [
	"aria-pressed",
	"disabled",
	"onClick"
], Uc = {
	key: 0,
	class: "tm-choice-card-icon",
	"aria-hidden": "true"
}, Wc = { class: "tm-choice-card-copy" }, Gc = { key: 0 }, Kc = {
	key: 1,
	class: "tm-option-sections"
}, qc = { class: "tm-option-grid" }, Jc = [
	"checked",
	"disabled",
	"onChange"
], Yc = {
	class: "tm-option-icon",
	"aria-hidden": "true"
}, Xc = { class: "tm-option-copy" }, Zc = { key: 0 }, Qc = {
	key: 2,
	class: "tm-option-grid"
}, $c = [
	"checked",
	"disabled",
	"onChange"
], el = { class: "tm-option-copy" }, tl = { key: 0 }, nl = { key: 3 }, rl = ["value", "disabled"], il = ["value"], al = { key: 0 }, ol = { class: "tm-range-row" }, sl = [
	"value",
	"min",
	"max",
	"step",
	"disabled"
], cl = [
	"value",
	"placeholder",
	"required",
	"disabled"
], ll = [
	"type",
	"value",
	"placeholder",
	"required",
	"disabled",
	"min",
	"max",
	"step"
], ul = { key: 2 }, dl = /* @__PURE__ */ sr({
	__name: "DynamicField",
	props: {
		field: {},
		modelValue: {},
		compact: { type: Boolean }
	},
	emits: ["update:modelValue", "change"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = Q(() => String(n.field.type || "text").toLowerCase()), a = Q(() => i.value === "player_multiselect"), o = Q(() => String(n.field.presentation || "").toLowerCase() === "cards"), s = Q(() => !!(n.field.disabled || n.field.read_only)), c = Q(() => String(n.modelValue ?? "")), l = Q(() => Number(n.modelValue ?? 0)), u = Q(() => new Set((Array.isArray(n.modelValue) ? n.modelValue : [n.modelValue]).map((e) => String(e ?? "")).filter(Boolean))), d = Q(() => Pc(n.field.options || [], (e) => f(e)));
		function f(e) {
			return String(e && typeof e == "object" ? e.value ?? e.id ?? e.key ?? e.label ?? "" : e ?? "");
		}
		function p(e) {
			return e && typeof e == "object" ? [
				e.label,
				e.title,
				e.name,
				e.friendly_name,
				e.description,
				e.meta,
				f(e)
			].map((e) => String(e ?? "").trim()).find(Boolean) || "Unnamed player" : String(e ?? "").trim() || "Unnamed player";
		}
		function m(e) {
			return Lc(p(e), f(e));
		}
		function h(e) {
			if (!e || typeof e != "object") return "";
			let t = p(e);
			return [
				e.description,
				e.meta,
				e.room,
				e.area
			].map((e) => String(e ?? "").trim()).find((e) => !!e && e !== t) || "";
		}
		function g(e) {
			return Rc(p(e), h(e), f(e));
		}
		function _(e) {
			return Nc(f(e));
		}
		function v(e) {
			if (e && typeof e == "object" && String(e.icon || "").trim()) return String(e.icon).trim();
			let t = _(e);
			return t === "stereo" ? "T²" : t === "satellite" ? "T" : t === "airplay" ? "△" : t === "sonos" ? "S" : t === "home" ? "H" : "♪";
		}
		function y(e) {
			let t = e.target, n;
			n = i.value === "checkbox" ? t.checked : i.value === "number" || i.value === "range" ? Number(t.value) : t.value, r("update:modelValue", n);
		}
		function b(e) {
			let t = e.target, n = i.value === "checkbox" ? t.checked : i.value === "number" || i.value === "range" ? Number(t.value) : t.value;
			r("update:modelValue", n), r("change", n);
		}
		function x(e, t) {
			let n = new Set(u.value);
			t ? n.add(e) : n.delete(e), r("update:modelValue", Array.from(n));
		}
		return (t, n) => i.value === "checkbox" ? (q(), J("label", {
			key: 0,
			class: B(["tm-field tm-checkbox", { compact: e.compact }])
		}, [Y("input", {
			type: "checkbox",
			checked: !!e.modelValue,
			disabled: s.value,
			onChange: y
		}, null, 40, zc), Y("span", null, [Y("strong", null, V(e.field.label || e.field.key), 1), e.field.description ? (q(), J("small", Bc, V(e.field.description), 1)) : Z("", !0)])], 2)) : i.value === "multiselect" || i.value === "player_multiselect" ? (q(), J("fieldset", {
			key: 1,
			class: B(["tm-field tm-multiselect", {
				compact: e.compact,
				"full-width": !!e.field.full_width,
				"tm-target-multiselect": a.value,
				"tm-choice-card-multiselect": o.value
			}])
		}, [
			Y("legend", null, V(e.field.label || e.field.key), 1),
			o.value ? (q(), J("div", Vc, [(q(!0), J(K, null, G(e.field.options || [], (e) => (q(), J("button", {
				key: f(e),
				type: "button",
				class: B(["tm-choice-card", { selected: u.value.has(f(e)) }]),
				"aria-pressed": u.value.has(f(e)),
				disabled: s.value || !f(e),
				onClick: (t) => x(f(e), !u.value.has(f(e)))
			}, [v(e) ? (q(), J("span", Uc, V(v(e)), 1)) : Z("", !0), Y("span", Wc, [Y("strong", null, V(p(e)), 1), h(e) ? (q(), J("small", Gc, V(h(e)), 1)) : Z("", !0)])], 10, Hc))), 128))])) : a.value ? (q(), J("div", Kc, [(q(!0), J(K, null, G(d.value, (e) => (q(), J("section", {
				key: e.key,
				class: "tm-option-section"
			}, [Y("h4", null, V(e.label), 1), Y("div", qc, [(q(!0), J(K, null, G(e.items, (e) => (q(), J("label", {
				key: f(e),
				class: B(["tm-option", [`kind-${_(e)}`, { "is-selected": u.value.has(f(e)) }]])
			}, [
				Y("input", {
					type: "checkbox",
					checked: u.value.has(f(e)),
					disabled: s.value || !f(e),
					onChange: (t) => x(f(e), t.target.checked)
				}, null, 40, Jc),
				Y("span", Yc, V(v(e)), 1),
				Y("span", Xc, [Y("strong", null, V(m(e)), 1), g(e) ? (q(), J("small", Zc, V(g(e)), 1)) : Z("", !0)])
			], 2))), 128))])]))), 128))])) : (q(), J("div", Qc, [(q(!0), J(K, null, G(e.field.options || [], (e) => (q(), J("label", {
				key: f(e),
				class: B(["tm-option", [`kind-${_(e)}`, { "is-selected": u.value.has(f(e)) }]])
			}, [Y("input", {
				type: "checkbox",
				checked: u.value.has(f(e)),
				disabled: s.value || !f(e),
				onChange: (t) => x(f(e), t.target.checked)
			}, null, 40, $c), Y("span", el, [Y("strong", null, V(p(e)), 1), h(e) ? (q(), J("small", tl, V(h(e)), 1)) : Z("", !0)])], 2))), 128))])),
			e.field.description ? (q(), J("small", nl, V(e.field.description), 1)) : Z("", !0)
		], 2)) : i.value === "select" ? (q(), J("label", {
			key: 2,
			class: B(["tm-field", { compact: e.compact }])
		}, [
			Y("span", null, V(e.field.label || e.field.key), 1),
			Y("select", {
				value: c.value,
				disabled: s.value,
				onChange: y
			}, [(q(!0), J(K, null, G(e.field.options || [], (e) => (q(), J("option", {
				key: f(e),
				value: f(e)
			}, V(p(e)), 9, il))), 128))], 40, rl),
			e.field.description ? (q(), J("small", al, V(e.field.description), 1)) : Z("", !0)
		], 2)) : i.value === "range" ? (q(), J("label", {
			key: 3,
			class: B(["tm-field tm-range", { compact: e.compact }])
		}, [Y("span", null, V(e.field.label || e.field.key), 1), Y("div", ol, [Y("input", {
			type: "range",
			value: l.value,
			min: e.field.min ?? 0,
			max: e.field.max ?? 100,
			step: e.field.step ?? 1,
			disabled: s.value,
			onInput: y,
			onChange: b
		}, null, 40, sl), Y("output", null, V(l.value) + V(e.field.suffix || ""), 1)])], 2)) : (q(), J("label", {
			key: 4,
			class: B(["tm-field", { compact: e.compact }])
		}, [
			Y("span", null, V(e.field.label || e.field.key), 1),
			i.value === "textarea" || i.value === "multiline" ? (q(), J("textarea", {
				key: 0,
				value: c.value,
				placeholder: e.field.placeholder,
				required: e.field.required,
				disabled: s.value,
				onInput: y
			}, null, 40, cl)) : (q(), J("input", {
				key: 1,
				type: i.value === "password" ? "password" : i.value === "number" ? "number" : "text",
				value: e.modelValue,
				placeholder: e.field.placeholder,
				required: e.field.required,
				disabled: s.value,
				min: e.field.min,
				max: e.field.max,
				step: e.field.step,
				onInput: y
			}, null, 40, ll)),
			e.field.description ? (q(), J("small", ul, V(e.field.description), 1)) : Z("", !0)
		], 2));
	}
}), fl = { class: "tm-library" }, pl = {
	key: 0,
	class: "tm-subtabs",
	"aria-label": "Browse music library"
}, ml = ["onClick"], hl = { class: "tm-search-controls" }, gl = ["disabled"], _l = {
	key: 0,
	class: "tm-library-grid"
}, vl = { class: "tm-library-art" }, yl = ["src", "alt"], bl = {
	key: 1,
	"aria-hidden": "true"
}, xl = [
	"disabled",
	"aria-label",
	"onClick"
], Sl = { class: "tm-library-copy" }, Cl = ["title"], wl = {
	key: 1,
	class: "tm-empty"
}, Tl = {
	key: 2,
	class: "tm-pagination",
	"aria-label": "Library pages"
}, El = ["disabled"], Dl = ["disabled"], Ol = /* @__PURE__ */ sr({
	__name: "LibraryBrowser",
	props: {
		groups: {},
		items: {},
		busy: {},
		run: {},
		selectedGroup: { default: "" },
		showNavigation: {
			type: Boolean,
			default: !0
		}
	},
	emits: ["update:selectedGroup"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ U(n.selectedGroup || n.groups[0]?.key || "search"), a = /* @__PURE__ */ U({}), o = /* @__PURE__ */ U({});
		On(() => n.groups, (e) => {
			e.some((e) => e.key === s.value) || g(e[0]?.key || "search");
		}, { deep: !0 }), On(() => n.selectedGroup, (e) => {
			e && e !== i.value && (i.value = e);
		});
		let s = Q(() => i.value), c = Q(() => n.groups.find((e) => e.key === s.value)), l = Q(() => {
			let e = c.value?.item_group || c.value?.key;
			return n.items.filter((t) => t.group === e);
		}), u = Q(() => l.value[0]), d = Q(() => Math.max(0, Number(c.value?.page_size || 0))), f = Q(() => Math.max(1, a.value[s.value] || 1)), p = Q(() => d.value ? Math.max(1, Math.ceil(l.value.length / d.value)) : 1), m = Q(() => {
			if (!d.value) return l.value;
			let e = (Math.min(f.value, p.value) - 1) * d.value;
			return l.value.slice(e, e + d.value);
		});
		On(u, (e) => {
			if (!e) return;
			let t = { ...o.value };
			for (let n of e.fields || []) n.key in t || (t[n.key] = n.value);
			o.value = t;
		}, { immediate: !0 });
		function h(e) {
			a.value = {
				...a.value,
				[s.value]: Math.max(1, Math.min(e, p.value))
			};
		}
		function g(e) {
			i.value = e, r("update:selectedGroup", e);
		}
		function _(e, t) {
			o.value = {
				...o.value,
				[e.key]: t
			};
		}
		async function v() {
			let e = u.value;
			e?.run_action && await n.run(e.run_action, {
				id: e.id,
				values: o.value
			}, `item:${e.id}`);
		}
		async function y(e) {
			e.run_action && await n.run(e.run_action, {
				id: e.id,
				values: {}
			}, `item:${e.id}`);
		}
		return (t, n) => (q(), J("section", fl, [e.showNavigation ? (q(), J("nav", pl, [(q(!0), J(K, null, G(e.groups, (e) => (q(), J("button", {
			key: e.key,
			type: "button",
			class: B({ active: s.value === e.key }),
			onClick: (t) => g(e.key)
		}, V(e.label || e.key), 11, ml))), 128))])) : Z("", !0), c.value?.key === "search" && u.value ? (q(), J("form", {
			key: 1,
			class: "tm-search",
			onSubmit: bs(v, ["prevent"])
		}, [Y("div", null, [
			n[2] ||= Y("div", { class: "tm-eyebrow" }, "Search across your connected library", -1),
			Y("h3", null, V(u.value.title || "Find music"), 1),
			Y("p", null, V(u.value.subtitle), 1)
		]), Y("div", hl, [(q(!0), J(K, null, G(u.value.fields || [], (e) => (q(), da(dl, {
			key: e.key,
			field: e,
			"model-value": o.value[e.key],
			compact: "",
			"onUpdate:modelValue": (t) => _(e, t)
		}, null, 8, [
			"field",
			"model-value",
			"onUpdate:modelValue"
		]))), 128)), Y("button", {
			type: "submit",
			class: "tm-button primary",
			disabled: e.busy(`item:${u.value.id}`)
		}, V(u.value.run_label || "Play Search"), 9, gl)])], 32)) : (q(), J(K, { key: 2 }, [m.value.length ? (q(), J("div", _l, [(q(!0), J(K, null, G(m.value, (t) => (q(), J("article", {
			key: t.id,
			class: "tm-library-card"
		}, [Y("div", vl, [t.hero_image_src ? (q(), J("img", {
			key: 0,
			src: t.hero_image_src,
			alt: t.hero_image_alt || "",
			loading: "lazy"
		}, null, 8, yl)) : (q(), J("span", bl, "♫")), t.run_action ? (q(), J("button", {
			key: 2,
			type: "button",
			disabled: e.busy(`item:${t.id}`),
			"aria-label": `${t.run_label || "Play"} ${t.title || ""}`,
			onClick: (e) => y(t)
		}, " ▶ ", 8, xl)) : Z("", !0)]), Y("div", Sl, [Y("strong", { title: t.title }, V(t.title || "Untitled"), 9, Cl), Y("small", null, V(t.subtitle), 1)])]))), 128))])) : (q(), J("div", wl, V(c.value?.empty_message || "Nothing is available here yet."), 1)), p.value > 1 ? (q(), J("div", Tl, [
			Y("button", {
				type: "button",
				disabled: f.value <= 1,
				onClick: n[0] ||= (e) => h(f.value - 1)
			}, "Previous", 8, El),
			Y("span", null, "Page " + V(Math.min(f.value, p.value)) + " of " + V(p.value), 1),
			Y("button", {
				type: "button",
				disabled: f.value >= p.value,
				onClick: n[1] ||= (e) => h(f.value + 1)
			}, "Next", 8, Dl)
		])) : Z("", !0)], 64))]));
	}
}), kl = /* @__PURE__ */ sr({
	__name: "PopupTransition",
	props: {
		open: { type: Boolean },
		backdropClass: { default: "tv-modal-backdrop" }
	},
	emits: ["close"],
	setup(e, { emit: t }) {
		let n = e, r = t;
		function i() {
			window.requestAnimationFrame(() => {
				let e = !!document.querySelector(".tater-popup-effect-backdrop");
				document.body.classList.toggle("modal-open", e);
			});
		}
		return On(() => n.open, (e) => {
			e && document.body.classList.add("modal-open");
		}, { immediate: !0 }), kr(i), (t, n) => (q(), da(Un, { to: "body" }, [ga(lo, {
			name: "tater-popup",
			appear: "",
			onBeforeEnter: i,
			onAfterLeave: i
		}, {
			default: Sn(() => [e.open ? (q(), J("div", {
				key: 0,
				class: B(["tater-popup-effect-backdrop", e.backdropClass]),
				onClick: n[0] ||= bs((e) => r("close"), ["self"])
			}, [
				n[1] ||= Y("span", {
					class: "tater-popup-effect-field",
					"aria-hidden": "true"
				}, null, -1),
				n[2] ||= Y("span", {
					class: "tater-popup-effect-burst",
					"aria-hidden": "true"
				}, null, -1),
				Vr(t.$slots, "default")
			], 2)) : Z("", !0)]),
			_: 3
		})]));
	}
}), Al = {
	class: "tm-player",
	"aria-label": "Music player"
}, jl = {
	id: "tm-player-details",
	class: "tm-player-main"
}, Ml = { class: "tm-art-wrap" }, Nl = ["src", "alt"], Pl = {
	key: 1,
	class: "tm-art tm-art-placeholder",
	"aria-hidden": "true"
}, Fl = { class: "tm-now-playing" }, Il = {
	class: "tm-transport",
	"aria-label": "Playback controls"
}, Ll = [
	"disabled",
	"aria-label",
	"title",
	"onClick"
], Rl = {
	key: 0,
	class: "tm-transport-play-icon",
	viewBox: "0 0 24 24",
	focusable: "false",
	"aria-hidden": "true"
}, zl = {
	key: 1,
	class: "tm-transport-glyph",
	"aria-hidden": "true"
}, Bl = { class: "tm-player-utility" }, Vl = ["value", "disabled"], Hl = ["aria-label"], Ul = { class: "tm-speaker-label" }, Wl = {
	class: "tm-modal",
	role: "dialog",
	"aria-modal": "true",
	"aria-labelledby": "tm-speaker-title"
}, Gl = { id: "tm-speaker-title" }, Kl = { class: "tm-modal-body" }, ql = {
	key: 0,
	class: "tm-player-rows"
}, Jl = { class: "tm-player-picker-intro" }, Yl = { class: "tm-player-section-list" }, Xl = { class: "tm-player-row-select" }, Zl = ["checked", "onChange"], Ql = {
	class: "tm-player-row-icon",
	"aria-hidden": "true"
}, $l = { class: "tm-player-row-copy" }, eu = { key: 0 }, tu = ["title"], nu = {
	key: 0,
	class: "tm-player-row-control tm-transport-mode-control"
}, ru = [
	"value",
	"disabled",
	"aria-label",
	"onChange"
], iu = ["value"], au = { class: "tm-player-row-control" }, ou = [
	"value",
	"disabled",
	"aria-label",
	"onInput"
], su = { class: "tm-player-modal-footer" }, cu = ["disabled"], lu = ["disabled"], uu = /* @__PURE__ */ sr({
	__name: "MusicPlayer",
	props: {
		item: {},
		busy: { type: Function },
		run: { type: Function }
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ U(!1), r = /* @__PURE__ */ U(75), i = /* @__PURE__ */ U({}), a = /* @__PURE__ */ U({}), o = /* @__PURE__ */ U(!1), s = /* @__PURE__ */ U(!1), c = Q(() => t.item.fields?.find((e) => e.key === "volume_percent")), l = Q(() => t.item.popup_fields || []), u = Q(() => t.item.player_rows || []), d = Q(() => Pc(u.value, (e) => e.target)), f = Q(() => ({ "--tm-volume-percent": `${Math.max(0, Math.min(100, r.value))}%` })), p = Q(() => v().length);
		function m(e) {
			return Array.isArray(e) ? e.map((e) => e && typeof e == "object" ? { ...e } : e) : e && typeof e == "object" ? { ...e } : e;
		}
		On(c, (e) => {
			e && !s.value && (r.value = Number(e.value ?? 75));
		}, { immediate: !0 }), On([l, u], ([e]) => {
			(!n.value || !o.value) && h(e);
		}, { immediate: !0 });
		function h(e = l.value) {
			i.value = Object.fromEntries(e.map((e) => [e.key, m(e.value)])), a.value = Object.fromEntries(u.value.map((e) => [e.target, {
				volume_percent: g(e.volume_percent, 75, 0, 100),
				sync_offset_ms: g(e.sync_offset_ms, 0, -1e3, 1e3),
				transport_mode: _(e.transport_mode)
			}]));
		}
		function g(e, t, n, r) {
			let i = Number(e);
			return Math.max(n, Math.min(r, Number.isFinite(i) ? i : t));
		}
		function _(e) {
			let t = String(e || "").toLowerCase();
			return t === "native" || t === "airplay" ? t : "auto";
		}
		function v() {
			let e = i.value.targets;
			return Array.isArray(e) ? e.map(String).filter(Boolean) : typeof e == "string" && e ? [e] : [];
		}
		function y(e) {
			return v().includes(e);
		}
		function b(e, t) {
			let n = v();
			i.value = {
				...i.value,
				targets: t ? Array.from(/* @__PURE__ */ new Set([...n, e])) : n.filter((t) => t !== e)
			}, o.value = !0;
		}
		function x(e) {
			return a.value[e] || {
				volume_percent: 75,
				sync_offset_ms: 0,
				transport_mode: "auto"
			};
		}
		function S(e, t) {
			let n = x(e);
			a.value = {
				...a.value,
				[e]: {
					...n,
					transport_mode: _(t.target.value)
				}
			}, o.value = !0;
		}
		function C(e, t) {
			let n = x(e);
			a.value = {
				...a.value,
				[e]: {
					...n,
					volume_percent: g(t.target.value, n.volume_percent, 0, 100)
				}
			}, o.value = !0;
		}
		function w(e) {
			if (e.sync_quality === "precise") return "Precise sync";
			if (e.sync_quality === "bridge") return "AirPlay bridge";
			if (e.sync_quality === "automatic") {
				let t = x(e.target).transport_mode;
				return t === "native" ? "Native Sonos" : t === "airplay" ? "AirPlay bridge" : "Auto sync";
			}
			return "Best effort";
		}
		function T(e) {
			return e.sync_quality === "precise" ? "Clock-scheduled Tater playback" : e.sync_quality === "bridge" ? "Wall-clock scheduled through Tater AirPlay Bridge" : e.sync_quality === "automatic" ? "Automatic uses AirPlay Bridge with Tater sats and native Sonos otherwise" : "Timing depends on the external player";
		}
		function E(e) {
			return Nc(e.target);
		}
		function D(e) {
			return Lc(e.label, e.target);
		}
		function O(e) {
			return Rc(e.label, e.meta, e.target);
		}
		function k(e) {
			let t = E(e);
			return t === "stereo" ? "T²" : t === "satellite" ? "T" : t === "airplay" ? "△" : t === "sonos" ? "S" : t === "home" ? "H" : "♪";
		}
		function A() {
			h(), o.value = !1, n.value = !0;
		}
		function j() {
			n.value = !1, o.value = !1, h();
		}
		function M(e) {
			return e.endsWith("_play") || e.endsWith("_pause") ? "primary" : e.endsWith("_stop") ? "stop" : "";
		}
		function N(e, t) {
			return e.endsWith("_previous") ? "⏮" : e.endsWith("_pause") ? "⏸" : e.endsWith("_stop") ? "■" : e.endsWith("_next") ? "⏭" : t;
		}
		async function P(e) {
			await t.run(e, {
				id: t.item.id,
				values: { volume_percent: r.value }
			}, "transport");
		}
		async function F() {
			let e = c.value;
			if (!e?.action) {
				s.value = !1;
				return;
			}
			let n = await t.run(e.action, {
				id: t.item.id,
				values: { volume_percent: r.value }
			}, "volume");
			s.value = !1, n || (r.value = Number(c.value?.value ?? r.value));
		}
		function I(e) {
			r.value = Number(e), s.value = !0;
		}
		function ee(e) {
			I(e.target.value);
		}
		async function te() {
			t.item.save_action && await t.run(t.item.save_action, {
				id: t.item.id,
				values: {
					...i.value,
					player_settings: a.value
				}
			}, "speakers") && (o.value = !1, h(), n.value = !1);
		}
		async function ne() {
			!t.item.test_sync_action || v().length === 0 || await t.run(t.item.test_sync_action, {
				id: t.item.id,
				values: {
					...i.value,
					player_settings: a.value
				}
			}, "sync-test");
		}
		function L(e, t) {
			i.value = {
				...i.value,
				[e.key]: t
			}, o.value = !0;
		}
		return (t, a) => {
			let o = Ir("DynamicField");
			return q(), J("section", Al, [Y("div", jl, [
				Y("div", Ml, [e.item.hero_image_src ? (q(), J("img", {
					key: 0,
					class: "tm-art",
					src: e.item.hero_image_src,
					alt: e.item.hero_image_alt || ""
				}, null, 8, Nl)) : (q(), J("div", Pl, "♫"))]),
				Y("div", Fl, [Y("h2", null, V(e.item.title || "Music Player"), 1), Y("p", null, V(e.item.subtitle || e.item.detail), 1)]),
				Y("div", Il, [(q(!0), J(K, null, G(e.item.actions || [], (t) => (q(), J("button", {
					key: t.action,
					type: "button",
					class: B([M(t.action), { "is-play": t.action.endsWith("_play") }]),
					disabled: e.busy("transport"),
					"aria-label": t.aria_label || t.label,
					title: t.tooltip || t.label,
					onClick: (e) => P(t.action)
				}, [t.action.endsWith("_play") ? (q(), J("svg", Rl, [...a[0] ||= [Y("path", { d: "M10 6.5 22 13.5 10 20.5Z" }, null, -1)]])) : (q(), J("span", zl, V(N(t.action, t.label || "Run")), 1))], 10, Ll))), 128))]),
				Y("div", Bl, [c.value ? (q(), J("label", {
					key: 0,
					class: "tm-player-volume",
					style: R(f.value)
				}, [
					a[1] ||= Y("span", { "aria-hidden": "true" }, "♪", -1),
					Y("input", {
						type: "range",
						min: "0",
						max: "100",
						step: "1",
						value: r.value,
						disabled: e.busy("volume"),
						"aria-label": "Music volume",
						onInput: ee,
						onChange: F
					}, null, 40, Vl),
					Y("output", null, V(r.value) + "%", 1)
				], 4)) : Z("", !0), Y("button", {
					type: "button",
					class: "tm-speaker-button",
					"aria-label": e.item.settings_aria_label || "Choose speakers and players",
					title: "Choose speakers and players",
					onClick: A
				}, [a[2] ||= Y("span", { "aria-hidden": "true" }, "🔊", -1), Y("span", Ul, V(p.value ? `${p.value} Player${p.value === 1 ? "" : "s"}` : "Players"), 1)], 8, Hl)])
			]), ga(kl, {
				open: n.value,
				"backdrop-class": "tm-modal-backdrop",
				onClose: j
			}, {
				default: Sn(() => [Y("section", Wl, [
					Y("header", null, [Y("div", null, [a[3] ||= Y("div", { class: "tm-eyebrow" }, "Playback destination", -1), Y("h3", Gl, V(e.item.settings_title || "Choose Speakers & Players"), 1)]), Y("button", {
						type: "button",
						class: "tm-close",
						"aria-label": "Close",
						onClick: j
					}, "×")]),
					Y("div", Kl, [u.value.length ? (q(), J("div", ql, [Y("div", Jl, [a[4] ||= Y("p", { class: "tm-player-calibration-help" }, " Pick any combination of Tater sats and external speakers. Selected players expand for playback route and volume. ", -1), Y("strong", null, V(v().length) + " selected", 1)]), (q(!0), J(K, null, G(d.value, (e) => (q(), J("section", {
						key: e.key,
						class: "tm-player-section"
					}, [Y("h4", null, V(e.label), 1), Y("div", Yl, [(q(!0), J(K, null, G(e.items, (e) => (q(), J("article", {
						key: e.target,
						class: B(["tm-player-row", [`kind-${E(e)}`, { "is-selected": y(e.target) }]])
					}, [Y("header", null, [Y("label", Xl, [
						Y("input", {
							type: "checkbox",
							checked: y(e.target),
							onChange: (t) => b(e.target, t.target.checked)
						}, null, 40, Zl),
						Y("span", Ql, V(k(e)), 1),
						Y("span", $l, [Y("strong", null, V(D(e)), 1), O(e) ? (q(), J("small", eu, V(O(e)), 1)) : Z("", !0)])
					]), Y("span", {
						class: B(["tm-sync-quality", `is-${e.sync_quality || "best_effort"}`]),
						title: T(e)
					}, V(w(e)), 11, tu)]), y(e.target) ? (q(), J("div", {
						key: 0,
						class: B(["tm-player-row-controls", { "has-transport": !!e.transport_options?.length }])
					}, [e.transport_options?.length ? (q(), J("label", nu, [Y("span", null, [a[5] ||= Y("strong", null, "Playback route", -1), Y("output", null, V(x(e.target).transport_mode === "auto" ? "Context aware" : "Fixed"), 1)]), Y("select", {
						value: x(e.target).transport_mode,
						disabled: !y(e.target),
						"aria-label": `${e.label || e.target} playback route`,
						onChange: (t) => S(e.target, t)
					}, [(q(!0), J(K, null, G(e.transport_options, (e) => (q(), J("option", {
						key: e.value,
						value: e.value
					}, V(e.label), 9, iu))), 128))], 40, ru)])) : Z("", !0), Y("label", au, [Y("span", null, [a[6] ||= Y("strong", null, "Volume", -1), Y("output", null, V(x(e.target).volume_percent) + "%", 1)]), Y("input", {
						type: "range",
						min: "0",
						max: "100",
						step: "1",
						value: x(e.target).volume_percent,
						disabled: !y(e.target),
						"aria-label": `${e.label || e.target} volume`,
						onInput: (t) => C(e.target, t)
					}, null, 40, ou)])], 2)) : Z("", !0)], 2))), 128))])]))), 128))])) : (q(!0), J(K, { key: 1 }, G(l.value, (e) => (q(), da(o, {
						key: e.key,
						field: e,
						"model-value": i.value[e.key],
						"onUpdate:modelValue": (t) => L(e, t)
					}, null, 8, [
						"field",
						"model-value",
						"onUpdate:modelValue"
					]))), 128))]),
					Y("footer", su, [
						e.item.test_sync_action && u.value.length ? (q(), J("button", {
							key: 0,
							type: "button",
							class: "tm-button secondary tm-sync-test",
							disabled: e.busy("sync-test") || v().length === 0,
							title: "Stops current music and plays a short click track",
							onClick: ne
						}, V(e.busy("sync-test") ? "Starting test…" : "Test sync"), 9, cu)) : Z("", !0),
						a[7] ||= Y("span", { class: "tm-modal-footer-spacer" }, null, -1),
						Y("button", {
							type: "button",
							class: "tm-button secondary",
							onClick: j
						}, "Cancel"),
						Y("button", {
							type: "button",
							class: "tm-button primary",
							disabled: e.busy("speakers") || v().length === 0,
							onClick: te
						}, " Set players ", 8, lu)
					])
				])]),
				_: 1
			}, 8, ["open"])]);
		};
	}
}), du = ["aria-label"], fu = { class: "tm-recommendations-heading" }, pu = { key: 0 }, mu = ["disabled"], hu = {
	key: 0,
	class: "tm-recommendation-grid"
}, gu = { class: "tm-recommendation-hero" }, _u = ["src", "alt"], vu = {
	key: 1,
	class: "tm-recommendation-placeholder",
	"aria-hidden": "true"
}, yu = {
	key: 2,
	class: "tm-badges"
}, bu = { class: "tm-recommendation-copy" }, xu = { class: "tm-eyebrow" }, Su = { class: "tm-recommendation-items" }, Cu = ["src", "alt"], wu = {
	key: 1,
	class: "tm-recommendation-entry-art",
	"aria-hidden": "true"
}, Tu = { key: 0 }, Eu = ["disabled", "onClick"], Du = {
	key: 1,
	class: "tm-empty tm-recommendations-empty"
}, Ou = /* @__PURE__ */ sr({
	__name: "RecommendationsBrowser",
	props: {
		items: {},
		busy: { type: Function },
		run: { type: Function }
	},
	setup(e) {
		let t = e, n = Q(() => t.items.find((e) => e.card_variant === "recommendations_intro")), r = Q(() => t.items.filter((e) => e.card_variant === "recommendation_playlist")), i = Q(() => String(n.value?.assistant_name || "Tater").trim() || "Tater"), a = Q(() => i.value.toLocaleLowerCase().endsWith("s") ? `${i.value}'` : `${i.value}'s`), o = Q(() => n.value?.title || `${a.value} Recommendations`);
		async function s() {
			let e = n.value;
			!e?.run_action || !e.refresh_available || await t.run(e.run_action, {
				id: e.id,
				values: {}
			}, "recommendations:refresh");
		}
		async function c(e) {
			e.run_action && await t.run(e.run_action, {
				id: e.id,
				values: {}
			}, `recommendations:${e.id}`);
		}
		return (t, a) => (q(), J("section", {
			class: "tm-recommendations",
			"aria-label": o.value
		}, [Y("header", fu, [Y("div", null, [
			a[0] ||= Y("div", { class: "tm-eyebrow" }, "Made for your ears", -1),
			Y("h2", null, V(o.value), 1),
			Y("p", null, V(n.value?.subtitle || "Named playlists shaped by what you listen to."), 1),
			n.value?.detail ? (q(), J("small", pu, V(n.value.detail), 1)) : Z("", !0)
		]), Y("button", {
			type: "button",
			class: "tm-button primary",
			disabled: !n.value?.refresh_available || e.busy("recommendations:refresh") || n.value?.refresh_running,
			onClick: s
		}, V(e.busy("recommendations:refresh") || n.value?.refresh_running ? `${i.value} is mixing…` : n.value?.run_label || "Refresh Recommendations"), 9, mu)]), r.value.length ? (q(), J("div", hu, [(q(!0), J(K, null, G(r.value, (t) => (q(), J("article", {
			key: t.id,
			class: "tm-recommendation-card"
		}, [
			Y("div", gu, [t.hero_image_src ? (q(), J("img", {
				key: 0,
				src: t.hero_image_src,
				alt: t.hero_image_alt || "",
				loading: "lazy"
			}, null, 8, _u)) : (q(), J("div", vu, "♫")), t.hero_badges?.length ? (q(), J("div", yu, [(q(!0), J(K, null, G(t.hero_badges, (e) => (q(), J("span", {
				key: e.label,
				class: B(`tone-${e.tone || "muted"}`)
			}, V(e.label), 3))), 128))])) : Z("", !0)]),
			Y("div", bu, [
				Y("div", xu, V(i.value) + " mix", 1),
				Y("h3", null, V(t.title || `${i.value} Mix`), 1),
				Y("p", null, V(t.subtitle), 1)
			]),
			Y("div", Su, [(q(!0), J(K, null, G(t.recommendation_items || [], (e) => (q(), J("div", {
				key: e.id,
				class: "tm-recommendation-entry"
			}, [e.image_src ? (q(), J("img", {
				key: 0,
				src: e.image_src,
				alt: e.image_alt || "",
				loading: "lazy"
			}, null, 8, Cu)) : (q(), J("span", wu, "♫")), Y("div", null, [
				Y("small", null, V(e.type === "album" ? `Album · ${e.track_count || 0} tracks` : "Song"), 1),
				Y("strong", null, V(e.title || "Untitled"), 1),
				Y("span", null, V([e.artist, e.type === "song" ? e.album : ""].filter(Boolean).join(" · ")), 1),
				e.reason ? (q(), J("p", Tu, V(e.reason), 1)) : Z("", !0)
			])]))), 128))]),
			Y("footer", null, [Y("button", {
				type: "button",
				class: "tm-button primary",
				disabled: e.busy(`recommendations:${t.id}`),
				onClick: (e) => c(t)
			}, V(e.busy(`recommendations:${t.id}`) ? "Starting…" : `▶ ${t.run_label || "Play Playlist"}`), 9, Eu), a[1] ||= Y("small", null, "Plays on the destinations selected above.", -1)])
		]))), 128))])) : (q(), J("div", Du, [a[2] ||= Y("strong", null, "No mixes yet", -1), Y("span", null, V(n.value?.detail || `Play some music and ${i.value} will start learning your taste.`), 1)]))], 8, du));
	}
}), ku = {
	key: 0,
	class: "tm-badges"
}, Au = {
	key: 0,
	class: "tm-card-detail"
}, ju = {
	key: 1,
	class: "tm-settings-summary"
}, Mu = { key: 4 }, Nu = ["disabled", "onClick"], Pu = ["disabled"], Fu = ["aria-label"], Iu = { class: "tm-eyebrow" }, Lu = { class: "tm-modal-body" }, Ru = ["disabled"], zu = /* @__PURE__ */ sr({
	__name: "SettingsCard",
	props: {
		item: {},
		busy: { type: Function },
		run: { type: Function },
		fieldsPopup: { type: Boolean },
		fieldsDropdown: { type: Boolean },
		dropdownLabel: {},
		popupLabel: {}
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ Et({}), r = /* @__PURE__ */ new Set(), i = /* @__PURE__ */ U(null), a = /* @__PURE__ */ U(!1), o = 0, s = null, c = "", l = Q(() => {
			let e = [], n = /* @__PURE__ */ new Set();
			for (let r of [...t.item.popup_fields || [], ...t.item.fields || []]) {
				let t = String(r.key || "").trim();
				!t || n.has(t) || (n.add(t), e.push(r));
			}
			return e;
		}), u = Q(() => t.fieldsPopup ? l.value : t.item.popup_fields || []), d = Q(() => u.value.length > 0);
		function f() {
			o = 0;
			let e = i.value;
			if (!e) return;
			let t = window.getComputedStyle(e), n = Number.parseFloat(t.gridAutoRows) || 8, r = Number.parseFloat(t.rowGap) || 13, a = Array.from(e.children).filter((e) => e instanceof HTMLElement);
			a.forEach((e) => {
				e.style.gridRowEnd = "auto";
			}), a.forEach((e) => {
				let t = e.getBoundingClientRect().height, i = Math.max(1, Math.ceil((t + r) / (n + r)));
				e.style.gridRowEnd = `span ${i}`;
			});
		}
		function p() {
			window.cancelAnimationFrame(o), o = window.requestAnimationFrame(f);
		}
		function m() {
			s?.disconnect();
			let e = i.value;
			e && (s = new ResizeObserver(p), s.observe(e), Array.from(e.children).forEach((e) => s?.observe(e)), p());
		}
		function h(e) {
			return Array.isArray(e) ? e.map((e) => e && typeof e == "object" ? { ...e } : e) : e && typeof e == "object" ? { ...e } : e;
		}
		function g(e) {
			return JSON.stringify((e || []).map((e) => ({
				key: e.key,
				type: e.type,
				label: e.label,
				description: e.description,
				placeholder: e.placeholder,
				compact: e.compact,
				disabled: e.disabled,
				read_only: e.read_only,
				required: e.required,
				min: e.min,
				max: e.max,
				step: e.step,
				suffix: e.suffix,
				options: e.options
			})));
		}
		function _(e) {
			for (let t of e) r.has(t.key) || (n[t.key] = h(t.value));
		}
		On(l, (e) => {
			_(e);
			let t = g(e);
			t !== c && (c = t, un().then(m));
		}, { immediate: !0 }), On(a, (e) => {
			e && un().then(m);
		}), Er(() => void un().then(m)), kr(() => {
			window.cancelAnimationFrame(o), s?.disconnect();
		});
		function v(e, t) {
			n[e.key] = t, r.add(e.key);
		}
		async function y(e = !1) {
			t.item.save_action && await t.run(t.item.save_action, {
				id: t.item.id,
				values: { ...n }
			}, `item:${t.item.id}:save`) && (r.clear(), _(l.value), e && (a.value = !1));
		}
		async function b(e) {
			e.confirm && !window.confirm(e.confirm) || await t.run(e.action, {
				id: t.item.id,
				values: { ...n }
			}, `item:${t.item.id}:${e.action}`);
		}
		return (t, r) => (q(), J(K, null, [Y("article", { class: B(["tm-settings-card", e.item.card_variant ? `variant-${e.item.card_variant}` : ""]) }, [
			Y("header", null, [Y("div", null, [Y("h3", null, V(e.item.title || e.item.id), 1), Y("p", null, V(e.item.subtitle), 1)]), e.item.hero_badges?.length ? (q(), J("div", ku, [(q(!0), J(K, null, G(e.item.hero_badges, (e) => (q(), J("span", {
				key: e.label,
				class: B(`tone-${e.tone || "muted"}`)
			}, V(e.label), 3))), 128))])) : Z("", !0)]),
			e.item.detail ? (q(), J("p", Au, V(e.item.detail), 1)) : Z("", !0),
			e.item.summary_rows?.length ? (q(), J("dl", ju, [(q(!0), J(K, null, G(e.item.summary_rows, (e) => (q(), J("div", { key: e.label }, [Y("dt", null, V(e.label), 1), Y("dd", null, V(e.value ?? "—"), 1)]))), 128))])) : Z("", !0),
			!e.fieldsPopup && e.fieldsDropdown && e.item.fields?.length ? (q(), J("details", {
				key: 2,
				class: "tm-settings-fields",
				onToggle: p
			}, [Y("summary", null, V(e.dropdownLabel || "Connection settings"), 1), Y("div", {
				ref_key: "fieldGrid",
				ref: i,
				class: "tm-form-grid"
			}, [(q(!0), J(K, null, G(e.item.fields, (e) => (q(), da(dl, {
				key: e.key,
				field: e,
				"model-value": n[e.key],
				compact: !!e.compact,
				"onUpdate:modelValue": (t) => v(e, t)
			}, null, 8, [
				"field",
				"model-value",
				"compact",
				"onUpdate:modelValue"
			]))), 128))], 512)], 32)) : !e.fieldsPopup && e.item.fields?.length ? (q(), J("div", {
				key: 3,
				ref_key: "fieldGrid",
				ref: i,
				class: "tm-form-grid"
			}, [(q(!0), J(K, null, G(e.item.fields, (e) => (q(), da(dl, {
				key: e.key,
				field: e,
				"model-value": n[e.key],
				compact: !!e.compact,
				"onUpdate:modelValue": (t) => v(e, t)
			}, null, 8, [
				"field",
				"model-value",
				"compact",
				"onUpdate:modelValue"
			]))), 128))], 512)) : Z("", !0),
			e.item.actions?.length || e.item.save_action || d.value ? (q(), J("footer", Mu, [
				(q(!0), J(K, null, G(e.item.actions || [], (t) => (q(), J("button", {
					key: t.action,
					type: "button",
					class: B(["tm-button", t.tone === "danger" ? "danger" : t.action.includes("activate") ? "primary" : "secondary"]),
					disabled: e.busy(`item:${e.item.id}:${t.action}`),
					onClick: (e) => b(t)
				}, V(t.label || "Run"), 11, Nu))), 128)),
				e.item.save_action && !e.fieldsPopup ? (q(), J("button", {
					key: 0,
					type: "button",
					class: "tm-button primary",
					disabled: e.busy(`item:${e.item.id}:save`),
					onClick: r[0] ||= (e) => y(!1)
				}, V(e.item.save_label || "Save"), 9, Pu)) : Z("", !0),
				d.value ? (q(), J("button", {
					key: 1,
					type: "button",
					class: "tm-button primary",
					"aria-label": e.item.settings_aria_label || e.popupLabel,
					onClick: r[1] ||= (e) => a.value = !0
				}, V(e.popupLabel || "Settings"), 9, Fu)) : Z("", !0)
			])) : Z("", !0)
		], 2), ga(kl, {
			open: a.value,
			"backdrop-class": "tm-modal-backdrop",
			onClose: r[5] ||= (e) => a.value = !1
		}, {
			default: Sn(() => [Y("form", {
				class: "tm-modal tm-settings-modal",
				onSubmit: r[4] ||= bs((e) => y(!0), ["prevent"])
			}, [
				Y("header", null, [Y("div", null, [Y("span", Iu, V(e.item.group || "Music settings"), 1), Y("h3", null, V(e.item.settings_title || `${e.item.title || e.item.id} Settings`), 1)]), Y("button", {
					class: "tm-button secondary",
					type: "button",
					onClick: r[2] ||= (e) => a.value = !1
				}, "Close")]),
				Y("div", Lu, [Y("div", {
					ref_key: "fieldGrid",
					ref: i,
					class: "tm-form-grid tm-modal-form-grid"
				}, [(q(!0), J(K, null, G(u.value, (e) => (q(), da(dl, {
					key: e.key,
					field: e,
					"model-value": n[e.key],
					compact: !!e.compact,
					"onUpdate:modelValue": (t) => v(e, t)
				}, null, 8, [
					"field",
					"model-value",
					"compact",
					"onUpdate:modelValue"
				]))), 128))], 512)]),
				Y("footer", null, [Y("button", {
					class: "tm-button secondary",
					type: "button",
					onClick: r[3] ||= (e) => a.value = !1
				}, "Cancel"), e.item.save_action ? (q(), J("button", {
					key: 0,
					class: "tm-button primary",
					type: "submit",
					disabled: e.busy(`item:${e.item.id}:save`)
				}, V(e.item.save_label || "Save"), 9, Ru)) : Z("", !0)])
			], 32)]),
			_: 1
		}, 8, ["open"])], 64));
	}
}), Bu = {
	class: "tm-queue tm-queue-tab",
	"aria-label": "Current playlist"
}, Vu = { class: "tm-queue-header" }, Hu = { class: "tm-queue-summary-actions" }, Uu = ["checked", "disabled"], Wu = {
	key: 0,
	class: "tm-track-scroll",
	role: "listbox",
	"aria-label": "Current track list"
}, Gu = [
	"disabled",
	"aria-current",
	"title",
	"onDblclick"
], Ku = { class: "tm-track-position" }, qu = ["src", "alt"], Ju = {
	key: 1,
	class: "tm-track-art placeholder",
	"aria-hidden": "true"
}, Yu = { class: "tm-track-copy" }, Xu = { class: "tm-track-duration" }, Zu = {
	key: 1,
	class: "tm-empty compact"
}, Qu = /* @__PURE__ */ sr({
	__name: "TrackList",
	props: {
		item: {},
		busy: { type: Function },
		run: { type: Function }
	},
	setup(e) {
		let t = e;
		async function n(e) {
			!t.item.track_list_action || !e.id || await t.run(t.item.track_list_action, {
				id: e.id,
				values: {}
			}, `track:${e.id}`);
		}
		async function r(e) {
			let n = e.target;
			t.item.track_list_shuffle_action && (await t.run(t.item.track_list_shuffle_action, {
				id: t.item.id,
				values: { shuffle: n.checked }
			}, "shuffle") || (n.checked = !n.checked));
		}
		return (t, i) => (q(), J("section", Bu, [Y("header", Vu, [Y("span", null, [Y("strong", null, V(e.item.track_list_label || "Playlist"), 1), Y("small", null, V(e.item.track_list?.length || 0) + " tracks", 1)]), Y("span", Hu, [Y("label", {
			class: "tm-shuffle",
			onClick: i[0] ||= bs(() => {}, ["stop"])
		}, [Y("input", {
			type: "checkbox",
			checked: !!e.item.track_list_shuffle,
			disabled: e.busy("shuffle"),
			onChange: r
		}, null, 40, Uu), i[1] ||= X(" Shuffle ", -1)])])]), e.item.track_list?.length ? (q(), J("div", Wu, [(q(!0), J(K, null, G(e.item.track_list, (t) => (q(), J("button", {
			key: t.id || t.position,
			type: "button",
			class: B(["tm-track", {
				active: t.active,
				pending: e.busy(`track:${t.id}`)
			}]),
			disabled: e.busy(`track:${t.id}`),
			"aria-current": t.active ? "true" : void 0,
			title: `Double-click to play ${t.title || "this track"}`,
			onDblclick: (e) => n(t)
		}, [
			Y("span", Ku, V(t.active ? "▶" : t.position), 1),
			t.image_src ? (q(), J("img", {
				key: 0,
				class: "tm-track-art",
				src: t.image_src,
				alt: t.image_alt || "",
				loading: "lazy"
			}, null, 8, qu)) : (q(), J("span", Ju, "♫")),
			Y("span", Yu, [Y("strong", null, V(t.title || "Untitled"), 1), Y("small", null, V([t.artist, t.album].filter(Boolean).join(" · ") || "Unknown artist"), 1)]),
			Y("span", Xu, V(t.duration || ""), 1)
		], 42, Gu))), 128))])) : (q(), J("div", Zu, "Play an album, artist, genre, or search to create a track list."))]));
	}
}), $u = { class: "tater-music-core" }, ed = {
	key: 0,
	class: "tm-error"
}, td = { class: "tm-page-heading" }, nd = ["title"], rd = {
	key: 0,
	class: "tm-stats",
	"aria-label": "Music library status"
}, id = {
	class: "tm-tabs",
	"aria-label": "Music Core sections"
}, ad = ["onClick"], od = {
	key: 1,
	class: "tm-subtabs tm-dock-subtabs",
	"aria-label": "Browse music library"
}, sd = ["onClick"], cd = {
	key: 0,
	class: "tm-empty"
}, ld = {
	key: 5,
	class: "tm-error-toast",
	role: "alert"
}, ud = {
	key: 6,
	class: "tm-success-toast",
	role: "status"
}, dd = /* @__PURE__ */ sr({
	__name: "MusicCoreApp",
	props: {
		state: {},
		options: {}
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ U(""), r = /* @__PURE__ */ U(/* @__PURE__ */ new Set()), i = /* @__PURE__ */ U(""), a = /* @__PURE__ */ U(""), o = /* @__PURE__ */ U("connecting"), s = null, c = 0, l = Q(() => t.state.payload || {}), u = Q(() => l.value.ui || {}), d = Q(() => u.value.item_forms || []), f = Q(() => d.value.find((e) => e.group === "player")), p = Q(() => u.value.manager_tabs || []), m = Q(() => p.value.find((e) => e.key === n.value) || p.value[0]), h = Q(() => m.value?.source === "grouped_items" && m.value.groups || []), g = /* @__PURE__ */ U(""), _ = Q(() => {
			let e = m.value;
			return !e || e.source === "grouped_items" ? [] : d.value.filter((t) => !e.item_group || t.group === e.item_group);
		});
		On(p, (e) => {
			if (!e.some((e) => e.key === n.value)) {
				let t = String(u.value.default_tab || "");
				n.value = e.some((e) => e.key === t) ? t : e[0]?.key || "";
			}
		}, {
			immediate: !0,
			deep: !0
		}), On(h, (e) => {
			e.some((e) => e.key === g.value) || (g.value = e[0]?.key || "");
		}, {
			immediate: !0,
			deep: !0
		});
		function v(e) {
			return r.value.has(e);
		}
		function y(e, t) {
			let n = new Set(r.value);
			t ? n.add(e) : n.delete(e), r.value = n;
		}
		async function b() {
			t.state.payload = await kc(t.options.tabEndpoint);
		}
		async function x(e, n, r = e) {
			if (!e || v(r)) return !1;
			i.value = "", a.value = "", y(r, !0);
			try {
				let r = await Ac(t.options.actionEndpoint, e, n);
				if (r.ok === !1) throw Error(String(r.message || r.detail || "Music action failed."));
				await b();
				let i = typeof r.message == "string" ? r.message.trim() : "";
				return i && (t.options.onToast ? t.options.onToast(i, "success") : a.value = i), !0;
			} catch (e) {
				return i.value = e instanceof Error ? e.message : String(e || "Music action failed."), !1;
			} finally {
				y(r, !1);
			}
		}
		function S() {
			s && s.close(), c && window.clearTimeout(c), o.value = "connecting", s = new EventSource(t.options.eventsEndpoint), s.addEventListener("core-tab", (e) => {
				try {
					t.state.payload = JSON.parse(e.data), o.value = "live";
				} catch {}
			}), s.addEventListener("open", () => {
				o.value = "live";
			}), s.addEventListener("error", () => {
				o.value = "offline", s?.close(), s = null, c = window.setTimeout(S, 3e3);
			});
		}
		return Er(S), kr(() => {
			c && window.clearTimeout(c), s?.close(), s = null;
		}), (e, t) => (q(), J("main", $u, [l.value.error ? (q(), J("div", ed, V(l.value.error), 1)) : (q(), J(K, { key: 1 }, [
			Y("header", td, [Y("div", null, [
				t[3] ||= Y("div", { class: "tm-eyebrow" }, "Tater Music", -1),
				Y("h1", null, V(u.value.title || "Music Core"), 1),
				Y("p", null, V(l.value.summary), 1)
			]), Y("div", {
				class: B(["tm-live-state", o.value]),
				title: `Music updates: ${o.value}`
			}, [t[4] ||= Y("span", null, null, -1), X(V(o.value === "live" ? "Live" : o.value === "connecting" ? "Connecting" : "Reconnecting"), 1)], 10, nd)]),
			l.value.stats?.length ? (q(), J("section", rd, [(q(!0), J(K, null, G(l.value.stats, (e) => (q(), J("div", { key: e.label }, [Y("span", null, V(e.label), 1), Y("strong", null, V(e.value ?? "—"), 1)]))), 128))])) : Z("", !0),
			Y("section", {
				class: B(["tm-playback-dock", { "has-player": f.value }]),
				"aria-label": "Playback and navigation"
			}, [
				f.value ? (q(), da(uu, {
					key: 0,
					item: f.value,
					busy: v,
					run: x
				}, null, 8, ["item"])) : Z("", !0),
				Y("nav", id, [(q(!0), J(K, null, G(p.value, (e) => (q(), J("button", {
					key: e.key,
					type: "button",
					class: B({ active: n.value === e.key }),
					onClick: (t) => n.value = e.key
				}, V(e.label || e.key), 11, ad))), 128))]),
				h.value.length ? (q(), J("nav", od, [(q(!0), J(K, null, G(h.value, (e) => (q(), J("button", {
					key: e.key,
					type: "button",
					class: B({ active: g.value === e.key }),
					onClick: (t) => g.value = e.key
				}, V(e.label || e.key), 11, sd))), 128))])) : Z("", !0)
			], 2),
			m.value?.source === "grouped_items" ? (q(), da(Ol, {
				key: 1,
				groups: m.value.groups || [],
				items: d.value,
				busy: v,
				run: x,
				"selected-group": g.value,
				"show-navigation": !1,
				"onUpdate:selectedGroup": t[0] ||= (e) => g.value = e
			}, null, 8, [
				"groups",
				"items",
				"selected-group"
			])) : m.value?.key === "recommendations" ? (q(), da(Ou, {
				key: 2,
				items: _.value,
				busy: v,
				run: x
			}, null, 8, ["items"])) : m.value?.source === "player_queue" && f.value ? (q(), da(Qu, {
				key: 3,
				item: f.value,
				busy: v,
				run: x
			}, null, 8, ["item"])) : (q(), J("section", {
				key: 4,
				class: B(["tm-settings-grid", `group-${m.value?.item_group || "all"}`])
			}, [(q(!0), J(K, null, G(_.value, (e) => (q(), da(zu, {
				key: e.id,
				item: e,
				busy: v,
				run: x,
				"fields-popup": !!u.value.item_fields_popup && e.fields_popup !== !1,
				"fields-dropdown": !!(e.fields_dropdown ?? u.value.item_fields_dropdown),
				"dropdown-label": e.fields_dropdown_label || u.value.item_fields_dropdown_label || "Connection settings",
				"popup-label": e.settings_label || u.value.item_fields_popup_label || "Settings"
			}, null, 8, [
				"item",
				"fields-popup",
				"fields-dropdown",
				"dropdown-label",
				"popup-label"
			]))), 128)), _.value.length ? Z("", !0) : (q(), J("div", cd, V(m.value?.empty_message || l.value.empty_message || "Nothing is available here yet."), 1))], 2)),
			i.value ? (q(), J("div", ld, [Y("span", null, V(i.value), 1), Y("button", {
				type: "button",
				"aria-label": "Dismiss",
				onClick: t[1] ||= (e) => i.value = ""
			}, "×")])) : Z("", !0),
			a.value ? (q(), J("div", ud, [Y("span", null, V(a.value), 1), Y("button", {
				type: "button",
				"aria-label": "Dismiss",
				onClick: t[2] ||= (e) => a.value = ""
			}, "×")])) : Z("", !0)
		], 64))]));
	}
}), fd = ["value"], pd = {
	key: 1,
	class: "tvf-section"
}, md = { key: 0 }, hd = {
	key: 2,
	class: "tvf-field full"
}, gd = { key: 0 }, _d = {
	key: 3,
	class: "tv-toggle tvf-toggle full"
}, vd = ["checked"], yd = { key: 0 }, bd = {
	key: 4,
	class: "tvf-field tvf-multiselect full"
}, xd = ["checked", "onChange"], Sd = { key: 0 }, Cd = {
	key: 5,
	class: "tvf-field"
}, wd = ["value"], Td = ["value"], Ed = { key: 0 }, Dd = {
	key: 6,
	class: "tvf-field full"
}, Od = [
	"value",
	"placeholder",
	"rows"
], kd = { key: 0 }, Ad = {
	key: 7,
	class: "tvf-field full"
}, jd = ["accept"], Md = { key: 0 }, Nd = {
	key: 8,
	class: "tvf-field"
}, Pd = { class: "tvf-range" }, Fd = [
	"value",
	"min",
	"max",
	"step"
], Id = { key: 0 }, Ld = [
	"type",
	"value",
	"min",
	"max",
	"step",
	"placeholder"
], Rd = { key: 0 }, zd = /* @__PURE__ */ sr({
	__name: "ManifestField",
	props: {
		field: {},
		modelValue: {},
		allValues: {}
	},
	emits: [
		"update:modelValue",
		"error",
		"notify"
	],
	setup(e, { emit: t }) {
		let n = e, r = t, i = Q(() => u(n.field.type || "text").toLowerCase()), a = Q(() => u(n.field.label || n.field.key || "Setting")), o = Q(() => String(n.modelValue ?? "")), s = Q(() => new Set(d(n.modelValue))), c = Q(() => (Array.isArray(n.field.show_when_all) ? n.field.show_when_all : n.field.show_when && typeof n.field.show_when == "object" ? [n.field.show_when] : []).every((e) => {
			let t = u(e.source_key ?? e.key);
			if (!t) return !0;
			let r = [
				...Array.isArray(e.any_of) ? e.any_of : [],
				...Array.isArray(e.values) ? e.values : [],
				...e.equals === void 0 ? [] : [e.equals],
				...e.value === void 0 ? [] : [e.value]
			].map((e) => String(e ?? "").trim());
			if (!r.length) return !0;
			let i = typeof n.allValues[t] == "boolean" ? n.allValues[t] ? "true" : "false" : String(n.allValues[t] ?? "").trim();
			return r.includes(i);
		})), l = Q(() => ["API_AUTH_KEY", "AUTH_TOKEN"].includes(u(n.field.key).toUpperCase()));
		function u(e) {
			return String(e ?? "").trim();
		}
		function d(e) {
			if (Array.isArray(e)) return e.map((e) => String(e ?? "")).filter(Boolean);
			let t = u(e);
			if (!t) return [];
			if (t.startsWith("[") && t.endsWith("]")) try {
				let e = JSON.parse(t);
				if (Array.isArray(e)) return e.map((e) => String(e ?? "")).filter(Boolean);
			} catch {}
			return t.split(",").map((e) => e.trim()).filter(Boolean);
		}
		function f(e) {
			if (e && typeof e == "object") {
				let t = e;
				return u(t.value ?? t.id ?? t.key ?? t.label);
			}
			return u(e);
		}
		function p(e) {
			if (e && typeof e == "object") {
				let t = e;
				return u(t.label ?? t.name ?? t.title ?? f(t));
			}
			return u(e);
		}
		function m(e) {
			let t = e.target;
			i.value === "checkbox" ? r("update:modelValue", t.checked) : i.value === "number" || i.value === "range" ? r("update:modelValue", t.value === "" ? "" : Number(t.value)) : r("update:modelValue", t.value);
		}
		function h(e, t) {
			let n = new Set(s.value);
			t ? n.add(e) : n.delete(e), r("update:modelValue", [...n]);
		}
		function g(e) {
			return new Promise((t, n) => {
				let r = new FileReader();
				r.onload = () => t(String(r.result || "")), r.onerror = () => n(/* @__PURE__ */ Error(`Could not read ${e.name}.`)), r.readAsDataURL(e);
			});
		}
		async function _(e) {
			let t = e.target, i = t.files?.[0];
			if (!i) return;
			let a = Number(n.field.max_bytes || 0);
			if (a > 0 && i.size > a) {
				r("error", `${i.name} is larger than ${Math.max(1, Math.floor(a / 1024 / 1024))} MB.`), t.value = "";
				return;
			}
			try {
				if (u(n.field.file_encoding || n.field.encoding).toLowerCase() === "base64") {
					let e = await g(i);
					r("update:modelValue", {
						filename: i.name || "upload.bin",
						content_type: i.type || "application/octet-stream",
						size: i.size,
						data_b64: e.slice(e.indexOf(",") + 1)
					});
				} else {
					let e = await i.text();
					(u(n.field.accept).toLowerCase().includes("json") || i.name.toLowerCase().endsWith(".json")) && JSON.parse(e), r("update:modelValue", e);
				}
			} catch (e) {
				r("error", e instanceof Error ? e.message : `Could not read ${i.name}.`);
			} finally {
				t.value = "";
			}
		}
		function v() {
			let e = /* @__PURE__ */ new Uint8Array(24);
			crypto.getRandomValues(e), r("update:modelValue", [...e].map((e) => e.toString(16).padStart(2, "0")).join(""));
		}
		async function y() {
			if (!o.value) {
				r("error", "No key to copy.");
				return;
			}
			try {
				await navigator.clipboard.writeText(o.value), r("notify", "Key copied.");
			} catch {
				r("error", "Clipboard is unavailable.");
			}
		}
		return (t, n) => c.value ? (q(), J(K, { key: 0 }, [i.value === "hidden" ? (q(), J("input", {
			key: 0,
			type: "hidden",
			value: o.value
		}, null, 8, fd)) : i.value === "section" || i.value === "header" ? (q(), J("section", pd, [Y("h3", null, V(a.value), 1), e.field.description ? (q(), J("p", md, V(e.field.description), 1)) : Z("", !0)])) : i.value === "readonly" || i.value === "read_only" ? (q(), J("label", hd, [
			Y("span", null, V(a.value), 1),
			Y("output", null, V(o.value), 1),
			e.field.description ? (q(), J("small", gd, V(e.field.description), 1)) : Z("", !0)
		])) : i.value === "checkbox" ? (q(), J("label", _d, [Y("input", {
			class: "tv-checkbox",
			type: "checkbox",
			checked: !!e.modelValue,
			onChange: m
		}, null, 40, vd), Y("span", null, [Y("strong", null, V(a.value), 1), e.field.description ? (q(), J("small", yd, V(e.field.description), 1)) : Z("", !0)])])) : i.value === "multiselect" ? (q(), J("fieldset", bd, [
			Y("legend", null, V(a.value), 1),
			Y("div", null, [(q(!0), J(K, null, G(e.field.options || [], (e) => (q(), J("label", { key: f(e) }, [Y("input", {
				class: "tv-checkbox",
				type: "checkbox",
				checked: s.value.has(f(e)),
				onChange: (t) => h(f(e), t.target.checked)
			}, null, 40, xd), Y("span", null, V(p(e)), 1)]))), 128))]),
			e.field.description ? (q(), J("small", Sd, V(e.field.description), 1)) : Z("", !0)
		])) : i.value === "select" ? (q(), J("label", Cd, [
			Y("span", null, V(a.value), 1),
			Y("select", {
				value: o.value,
				onChange: m
			}, [(q(!0), J(K, null, G(e.field.options || [], (e) => (q(), J("option", {
				key: f(e),
				value: f(e)
			}, V(p(e)), 9, Td))), 128))], 40, wd),
			e.field.description ? (q(), J("small", Ed, V(e.field.description), 1)) : Z("", !0)
		])) : i.value === "textarea" || i.value === "multiline" ? (q(), J("label", Dd, [
			Y("span", null, V(a.value), 1),
			Y("textarea", {
				value: o.value,
				placeholder: e.field.placeholder,
				rows: e.field.rows || 4,
				onInput: m
			}, null, 40, Od),
			e.field.description ? (q(), J("small", kd, V(e.field.description), 1)) : Z("", !0)
		])) : i.value === "file" ? (q(), J("label", Ad, [
			Y("span", null, V(a.value), 1),
			Y("input", {
				type: "file",
				accept: e.field.accept,
				onChange: _
			}, null, 40, jd),
			Y("small", null, V(e.modelValue ? "A saved value is present. Choose a file to replace it." : "No file saved."), 1),
			e.field.description ? (q(), J("small", Md, V(e.field.description), 1)) : Z("", !0)
		])) : i.value === "range" ? (q(), J("label", Nd, [
			Y("span", null, V(a.value), 1),
			Y("div", Pd, [Y("input", {
				type: "range",
				value: Number(e.modelValue ?? e.field.default ?? 0),
				min: e.field.min ?? 0,
				max: e.field.max ?? 100,
				step: e.field.step ?? 1,
				onInput: m
			}, null, 40, Fd), Y("output", null, V(e.modelValue) + V(e.field.suffix || ""), 1)]),
			e.field.description ? (q(), J("small", Id, V(e.field.description), 1)) : Z("", !0)
		])) : (q(), J("label", {
			key: 9,
			class: B(["tvf-field", { full: e.field.full_width }])
		}, [
			Y("span", null, V(a.value), 1),
			Y("div", { class: B({ "tvf-input-actions": l.value }) }, [Y("input", {
				type: [
					"password",
					"number",
					"color",
					"time",
					"email",
					"url"
				].includes(i.value) ? i.value : "text",
				value: e.modelValue,
				min: e.field.min,
				max: e.field.max,
				step: e.field.step,
				placeholder: e.field.placeholder,
				onInput: m
			}, null, 40, Ld), l.value ? (q(), J(K, { key: 0 }, [Y("button", {
				class: "tv-button",
				type: "button",
				onClick: y
			}, "Copy"), Y("button", {
				class: "tv-button",
				type: "button",
				onClick: v
			}, "Generate")], 64)) : Z("", !0)], 2),
			e.field.description ? (q(), J("small", Rd, V(e.field.description), 1)) : Z("", !0)
		], 2))], 64)) : Z("", !0);
	}
}), Bd = {
	key: 0,
	class: "core-builder-step-heading"
}, Vd = { class: "core-builder-step-title" }, Hd = {
	key: 0,
	class: "small"
}, Ud = ["value"], Wd = { key: 2 }, Gd = { class: "core-data-table-wrap" }, Kd = { class: "core-data-table" }, qd = { key: 0 }, Jd = ["colspan"], Yd = { key: 0 }, Xd = { key: 3 }, Zd = { class: "core-bar-chart" }, Qd = { class: "core-bar-label" }, $d = { class: "core-bar-track" }, ef = { class: "core-bar-value" }, tf = {
	key: 0,
	class: "small"
}, nf = { key: 0 }, rf = {
	key: 4,
	class: "core-image-checklist"
}, af = { class: "core-image-checklist-head" }, of = { class: "small" }, sf = { class: "core-image-check-grid" }, cf = ["disabled", "onClick"], lf = ["src", "alt"], uf = { class: "core-image-check-copy" }, df = { key: 0 }, ff = { key: 1 }, pf = { key: 0 }, mf = { key: 5 }, hf = { key: 0 }, gf = ["src", "alt"], _f = {
	key: 2,
	class: "small"
}, vf = { key: 3 }, yf = { key: 4 }, bf = { key: 6 }, xf = { key: 0 }, Sf = {
	key: 1,
	class: "tcx-native-video-shell"
}, Cf = [
	"src",
	"poster",
	"controls",
	"preload"
], wf = ["aria-label"], Tf = ["src", "alt"], Ef = {
	key: 2,
	class: "small"
}, Df = { key: 3 }, Of = { key: 4 }, kf = ["checked", "disabled"], Af = { key: 0 }, jf = {
	key: 8,
	class: "core-range-field"
}, Mf = { class: "core-range-field-head" }, Nf = [
	"value",
	"min",
	"max",
	"step",
	"disabled"
], Pf = { key: 0 }, Ff = {
	key: 9,
	class: "core-choice-card-field"
}, If = { class: "core-choice-card-field-label" }, Lf = [
	"aria-pressed",
	"disabled",
	"onClick"
], Rf = {
	key: 0,
	class: "core-choice-card-icon",
	"aria-hidden": "true"
}, zf = { class: "core-choice-card-copy" }, Bf = { key: 0 }, Vf = { key: 1 }, Hf = { key: 0 }, Uf = { key: 10 }, Wf = ["size", "disabled"], Gf = ["value", "selected"], Kf = { key: 0 }, qf = { key: 11 }, Jf = ["value", "disabled"], Yf = ["label"], Xf = ["value"], Zf = ["value"], Qf = { key: 0 }, $f = { key: 12 }, ep = { key: 0 }, tp = {
	key: 1,
	class: "core-readonly-text"
}, np = [
	"value",
	"rows",
	"placeholder",
	"disabled"
], rp = { key: 3 }, ip = { key: 13 }, ap = [
	"accept",
	"capture",
	"disabled"
], op = {
	key: 0,
	class: "inline-row"
}, sp = { key: 2 }, cp = { key: 14 }, lp = { key: 0 }, up = {
	key: 1,
	class: "core-readonly-text"
}, dp = [
	"type",
	"value",
	"min",
	"max",
	"step",
	"placeholder",
	"disabled"
], fp = { key: 3 }, pp = { key: 4 }, mp = /* @__PURE__ */ sr({
	__name: "CoreManagerField",
	props: {
		field: {},
		modelValue: {},
		allValues: {}
	},
	emits: [
		"update:modelValue",
		"commit",
		"error"
	],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ U(!1), a = /* @__PURE__ */ U(null), o = /* @__PURE__ */ U(null), s = /* @__PURE__ */ U(!1), c = null, l = !1, u = !1, d = Q(() => D(n.field.type || "text").toLowerCase()), f = Q(() => D(n.field.presentation || n.field.display).toLowerCase()), p = Q(() => D(n.field.label || n.field.key || "Field")), m = Q(() => String(n.modelValue ?? "")), h = Q(() => new Set(A(n.modelValue))), g = Q(() => M(n.field.show_when_all, n.field.show_when, !0)), _ = Q(() => M(n.field.disable_when_all, n.field.disable_when, !1)), v = Q(() => !!(n.field.disabled || _.value)), y = Q(() => !!(n.field.read_only || n.field.readonly || ["readonly", "read_only"].includes(d.value))), b = Q(() => !!n.field.full_width || [
			"heading",
			"section_heading",
			"table",
			"bar_chart",
			"bars",
			"image_checklist",
			"image",
			"video",
			"file"
		].includes(d.value)), x = Q(() => {
			let e = n.field.dependent_options && typeof n.field.dependent_options == "object" ? n.field.dependent_options : null;
			if (!e) return Array.isArray(n.field.options) ? n.field.options : [];
			let t = D(e.source_key), r = k(n.allValues[t]), i = e.options_by_source && typeof e.options_by_source == "object" ? e.options_by_source[r] : null;
			return Array.isArray(i) && i.length ? i : Array.isArray(e.default_options) ? e.default_options : Array.isArray(n.field.options) ? n.field.options : [];
		}), S = Q(() => ["choice_cards", "multi_choice_cards"].includes(d.value) || f.value === "cards" && ["select", "multiselect"].includes(d.value)), C = Q(() => [
			"multi_choice_cards",
			"multiselect",
			"image_checklist"
		].includes(d.value)), w = Q(() => (Array.isArray(n.field.columns) ? n.field.columns : []).map((e, t) => {
			if (e && typeof e == "object") {
				let n = e, r = D(n.key ?? n.id ?? n.field ?? `col_${t}`) || `col_${t}`;
				return {
					key: r,
					label: D(n.label || r) || r
				};
			}
			let n = D(e) || `col_${t}`;
			return {
				key: n,
				label: n
			};
		})), T = Q(() => {
			let e = (Array.isArray(n.field.points) ? n.field.points : []).map((e) => {
				if (e && typeof e == "object") {
					let t = e;
					return {
						label: D(t.label ?? t.key ?? t.name) || "item",
						value: O(t.value ?? t.count)
					};
				}
				return {
					label: D(e) || "item",
					value: 0
				};
			}), t = e.reduce((e, t) => Math.max(e, t.value), 0);
			return e.map((e) => ({
				...e,
				width: t > 0 ? Math.max(0, Math.min(100, e.value / t * 100)) : 0
			}));
		}), E = Q(() => (Array.isArray(n.field.options) ? n.field.options : []).map((e, t) => {
			let n = e && typeof e == "object" ? e : {}, r = D(n.value ?? n.id);
			return {
				value: r,
				src: D(n.src ?? n.url),
				alt: D(n.alt) || `${p.value} image ${t + 1}`,
				caption: D(n.caption),
				meta: D(n.meta ?? n.description),
				selectable: n.selectable === void 0 ? !!r : !!(n.selectable && r)
			};
		}).filter((e) => e.src));
		function D(e) {
			return String(e ?? "").trim();
		}
		function O(e) {
			let t = Number(e);
			return Number.isFinite(t) ? t : 0;
		}
		function k(e) {
			return typeof e == "boolean" ? e ? "true" : "false" : D(e);
		}
		function A(e) {
			if (Array.isArray(e)) return e.map((e) => String(e ?? "")).filter(Boolean);
			let t = D(e);
			if (!t) return [];
			try {
				let e = JSON.parse(t);
				if (Array.isArray(e)) return e.map((e) => String(e ?? "")).filter(Boolean);
			} catch {}
			return t.split(",").map((e) => e.trim()).filter(Boolean);
		}
		function j(e, t) {
			return Array.isArray(e) ? e.filter((e) => e && typeof e == "object") : t && typeof t == "object" ? [t] : [];
		}
		function M(e, t, r) {
			let i = j(e, t);
			return i.length ? i.every((e) => {
				let t = D(e.source_key ?? e.key);
				if (!t) return r;
				let i = [
					...Array.isArray(e.any_of) ? e.any_of : [],
					...Array.isArray(e.values) ? e.values : [],
					...e.equals === void 0 ? [] : [e.equals],
					...e.eq === void 0 ? [] : [e.eq],
					...e.value === void 0 ? [] : [e.value]
				].map(k);
				return i.length ? i.includes(k(n.allValues[t])) : r;
			}) : r;
		}
		function N(e) {
			if (e && typeof e == "object") {
				let t = e;
				return String(t.value ?? t.id ?? t.key ?? t.label ?? "");
			}
			return String(e ?? "");
		}
		function P(e) {
			if (e && typeof e == "object") {
				let t = e;
				return D(t.label ?? t.title ?? t.name ?? N(e)) || N(e);
			}
			return String(e ?? "");
		}
		function F(e, t) {
			if (!e || typeof e != "object") return "";
			let n = e;
			return D(t === "description" ? n.description ?? n.subtitle : t === "meta" ? n.meta ?? n.detail : n.icon);
		}
		function I(e) {
			let t = e.target;
			return d.value === "checkbox" ? t.checked : ["number", "range"].includes(d.value) ? t.value === "" ? "" : Number(t.value) : C.value && t instanceof HTMLSelectElement ? [...t.selectedOptions].map((e) => e.value).filter(Boolean) : t.value;
		}
		function ee(e) {
			r("update:modelValue", I(e));
		}
		function te(e) {
			let t = I(e);
			r("update:modelValue", t), r("commit", t);
		}
		function ne(e) {
			if (v.value || y.value) return;
			if (!C.value) {
				r("update:modelValue", e);
				return;
			}
			let t = new Set(h.value);
			t.has(e) ? t.delete(e) : t.add(e), r("update:modelValue", [...t]);
		}
		function L(e, t, n) {
			return Array.isArray(e) ? e[n] ?? "" : e && typeof e == "object" ? e[t] ?? "" : "";
		}
		function z(e) {
			return new Promise((t, n) => {
				let r = new FileReader();
				r.onload = () => t(String(r.result || "")), r.onerror = () => n(/* @__PURE__ */ Error(`Could not read ${e.name}.`)), r.readAsDataURL(e);
			});
		}
		async function re(e) {
			let t = Number(n.field.max_bytes || 0);
			if (t > 0 && e.size > t) {
				r("error", `${e.name} is larger than ${Math.max(1, Math.floor(t / 1024 / 1024))} MB.`);
				return;
			}
			try {
				if (D(n.field.file_encoding || n.field.encoding).toLowerCase() === "base64") {
					let t = await z(e);
					r("update:modelValue", {
						filename: e.name || "upload.bin",
						content_type: e.type || "application/octet-stream",
						size: e.size,
						data_b64: t.slice(t.indexOf(",") + 1)
					});
				} else r("update:modelValue", await e.text());
			} catch (t) {
				r("error", t instanceof Error ? t.message : `Could not read ${e.name}.`);
			}
		}
		async function ie(e) {
			let t = e.target, n = t.files?.[0];
			n && await re(n), t.value = "";
		}
		async function ae() {
			if (!navigator.mediaDevices?.getUserMedia) {
				r("error", "Camera access is unavailable in this browser.");
				return;
			}
			try {
				c = await navigator.mediaDevices.getUserMedia({
					video: { facingMode: D(n.field.camera_facing_mode) === "environment" ? "environment" : "user" },
					audio: !1
				}), i.value = !0, requestAnimationFrame(() => {
					a.value && (a.value.srcObject = c);
				});
			} catch (e) {
				r("error", e instanceof Error ? e.message : "Could not open the camera.");
			}
		}
		function oe() {
			c?.getTracks().forEach((e) => e.stop()), c = null, i.value = !1;
		}
		async function se() {
			let e = a.value;
			if (!e || !e.videoWidth || !e.videoHeight) return;
			let t = document.createElement("canvas");
			t.width = e.videoWidth, t.height = e.videoHeight, t.getContext("2d")?.drawImage(e, 0, 0);
			let n = await new Promise((e) => t.toBlob(e, "image/jpeg", .9));
			n && await re(new File([n], `camera-${Date.now()}.jpg`, { type: "image/jpeg" })), oe();
		}
		function ce() {
			return !!(n.field.reset_to_poster && (n.field.poster || n.field.poster_src));
		}
		function le() {
			let e = o.value;
			if (!(!e || !ce() || !l)) {
				u = !0, l = !1, e.paused || e.pause();
				try {
					e.currentTime = 0;
				} catch {}
				s.value = !0, u = !1;
			}
		}
		function ue() {
			l = !0, s.value = !1;
		}
		function de(e) {
			let t = e.target;
			!u && !t.ended && le();
		}
		async function fe() {
			let e = o.value;
			if (e) {
				s.value = !1;
				try {
					e.currentTime = 0, await e.play();
				} catch {
					s.value = !0;
				}
			}
		}
		return kr(oe), (t, n) => g.value ? (q(), J("div", {
			key: 0,
			class: B(["tcx-native-field", [{
				full: b.value,
				muted: _.value
			}, `type-${d.value}`]])
		}, [d.value === "heading" || d.value === "section_heading" ? (q(), J("section", Bd, [Y("div", Vd, V(p.value), 1), e.field.description ? (q(), J("div", Hd, V(e.field.description), 1)) : Z("", !0)])) : d.value === "hidden" ? (q(), J("input", {
			key: 1,
			type: "hidden",
			value: m.value
		}, null, 8, Ud)) : d.value === "table" ? (q(), J("label", Wd, [
			Y("span", null, V(p.value), 1),
			Y("div", Gd, [Y("table", Kd, [Y("thead", null, [Y("tr", null, [(q(!0), J(K, null, G(w.value, (e) => (q(), J("th", { key: e.key }, V(e.label), 1))), 128))])]), Y("tbody", null, [(q(!0), J(K, null, G(e.field.rows || [], (e, t) => (q(), J("tr", { key: t }, [(q(!0), J(K, null, G(w.value, (t, n) => (q(), J("td", { key: t.key }, V(L(e, t.key, n)), 1))), 128))]))), 128)), (e.field.rows || []).length ? Z("", !0) : (q(), J("tr", qd, [Y("td", {
				colspan: Math.max(1, w.value.length),
				class: "small"
			}, "No rows.", 8, Jd)]))])])]),
			e.field.description ? (q(), J("small", Yd, V(e.field.description), 1)) : Z("", !0)
		])) : d.value === "bar_chart" || d.value === "bars" ? (q(), J("label", Xd, [
			Y("span", null, V(p.value), 1),
			Y("div", Zd, [(q(!0), J(K, null, G(T.value, (e) => (q(), J("div", {
				key: e.label,
				class: "core-bar-row"
			}, [
				Y("div", Qd, V(e.label), 1),
				Y("div", $d, [Y("div", {
					class: "core-bar-fill",
					style: R({ width: `${e.width}%` })
				}, null, 4)]),
				Y("div", ef, V(e.value), 1)
			]))), 128)), T.value.length ? Z("", !0) : (q(), J("div", tf, "No chart data."))]),
			e.field.description ? (q(), J("small", nf, V(e.field.description), 1)) : Z("", !0)
		])) : d.value === "image_checklist" ? (q(), J("div", rf, [
			Y("div", af, [Y("strong", null, V(p.value), 1), Y("span", of, V(E.value.length) + " image" + V(E.value.length === 1 ? "" : "s"), 1)]),
			Y("div", sf, [(q(!0), J(K, null, G(E.value, (e) => (q(), J("button", {
				key: e.src,
				type: "button",
				class: B(["core-image-check-card", {
					selected: h.value.has(e.value),
					"read-only": !e.selectable
				}]),
				disabled: v.value || y.value || !e.selectable,
				onClick: (t) => ne(e.value)
			}, [Y("img", {
				src: e.src,
				alt: e.alt,
				loading: "lazy"
			}, null, 8, lf), Y("span", uf, [e.caption ? (q(), J("strong", df, V(e.caption), 1)) : Z("", !0), e.meta ? (q(), J("small", ff, V(e.meta), 1)) : Z("", !0)])], 10, cf))), 128))]),
			e.field.description ? (q(), J("small", pf, V(e.field.description), 1)) : Z("", !0)
		])) : d.value === "image" ? (q(), J("div", mf, [
			e.field.hide_label ? Z("", !0) : (q(), J("strong", hf, V(p.value), 1)),
			e.field.src || e.field.url ? (q(), J("img", {
				key: 1,
				class: "tcx-native-media",
				src: e.field.src || e.field.url,
				alt: e.field.alt || p.value,
				loading: "lazy"
			}, null, 8, gf)) : (q(), J("div", _f, "No image available.")),
			e.field.caption ? (q(), J("small", vf, V(e.field.caption), 1)) : Z("", !0),
			e.field.description ? (q(), J("small", yf, V(e.field.description), 1)) : Z("", !0)
		])) : d.value === "video" ? (q(), J("div", bf, [
			e.field.hide_label ? Z("", !0) : (q(), J("strong", xf, V(p.value), 1)),
			e.field.src || e.field.url ? (q(), J("div", Sf, [W(Y("video", {
				ref_key: "mediaVideo",
				ref: o,
				class: "tcx-native-media",
				src: e.field.src || e.field.url,
				poster: e.field.poster || e.field.poster_src,
				controls: e.field.controls !== !1,
				preload: e.field.preload || "metadata",
				playsinline: "",
				onPlay: ue,
				onPause: de,
				onEnded: le
			}, null, 40, Cf), [[Oo, !s.value]]), ce() ? W((q(), J("button", {
				key: 0,
				type: "button",
				class: "tcx-native-video-poster",
				"aria-label": `Play ${p.value || "event clip"}`,
				onClick: fe
			}, [Y("img", {
				src: e.field.poster || e.field.poster_src,
				alt: p.value || "Event clip snapshot"
			}, null, 8, Tf), n[0] ||= Y("span", { "aria-hidden": "true" }, "▶", -1)], 8, wf)), [[Oo, s.value]]) : Z("", !0)])) : (q(), J("div", Ef, "No video available.")),
			e.field.caption ? (q(), J("small", Df, V(e.field.caption), 1)) : Z("", !0),
			e.field.description ? (q(), J("small", Of, V(e.field.description), 1)) : Z("", !0)
		])) : d.value === "checkbox" ? (q(), J("label", {
			key: 7,
			class: B(["tcx-native-toggle", { compact: f.value === "compact" || f.value === "compact_toggle" }])
		}, [Y("input", {
			class: "toggle-input",
			type: "checkbox",
			checked: !!e.modelValue,
			disabled: v.value,
			onChange: ee
		}, null, 40, kf), Y("span", null, [Y("strong", null, V(p.value), 1), e.field.description ? (q(), J("small", Af, V(e.field.description), 1)) : Z("", !0)])], 2)) : d.value === "range" ? (q(), J("label", jf, [
			Y("span", Mf, [Y("span", null, V(p.value), 1), Y("output", null, V(e.modelValue ?? 0) + V(e.field.suffix || ""), 1)]),
			Y("input", {
				type: "range",
				value: Number(e.modelValue ?? 0),
				min: e.field.min ?? 0,
				max: e.field.max ?? 100,
				step: e.field.step ?? 1,
				disabled: v.value || y.value,
				onInput: ee,
				onChange: te
			}, null, 40, Nf),
			e.field.description ? (q(), J("small", Pf, V(e.field.description), 1)) : Z("", !0)
		])) : S.value ? (q(), J("div", Ff, [
			Y("div", If, V(p.value), 1),
			Y("div", { class: B(["core-choice-card-grid", { "is-multiple": C.value }]) }, [(q(!0), J(K, null, G(x.value, (e) => (q(), J("button", {
				key: N(e),
				type: "button",
				class: B(["core-choice-card", { selected: h.value.has(N(e)) }]),
				"aria-pressed": h.value.has(N(e)),
				disabled: v.value || y.value,
				onClick: (t) => ne(N(e))
			}, [F(e, "icon") ? (q(), J("span", Rf, V(F(e, "icon")), 1)) : Z("", !0), Y("span", zf, [
				Y("strong", null, V(P(e)), 1),
				F(e, "description") ? (q(), J("span", Bf, V(F(e, "description")), 1)) : Z("", !0),
				F(e, "meta") ? (q(), J("small", Vf, V(F(e, "meta")), 1)) : Z("", !0)
			])], 10, Lf))), 128))], 2),
			e.field.description ? (q(), J("small", Hf, V(e.field.description), 1)) : Z("", !0)
		])) : d.value === "multiselect" ? (q(), J("label", Uf, [
			Y("span", null, V(p.value), 1),
			Y("select", {
				multiple: "",
				size: Math.max(4, Math.min(10, x.value.length || 6)),
				disabled: v.value || y.value,
				onChange: ee
			}, [(q(!0), J(K, null, G(x.value, (e) => (q(), J("option", {
				key: N(e),
				value: N(e),
				selected: h.value.has(N(e))
			}, V(P(e)), 9, Gf))), 128))], 40, Wf),
			e.field.description ? (q(), J("small", Kf, V(e.field.description), 1)) : Z("", !0)
		])) : d.value === "select" ? (q(), J("label", qf, [
			Y("span", null, V(p.value), 1),
			Y("select", {
				value: m.value,
				disabled: v.value || y.value,
				onChange: ee
			}, [(q(!0), J(K, null, G(x.value, (e, t) => (q(), J(K, { key: N(e) || t }, [e && typeof e == "object" && Array.isArray(e.options) && e.value === void 0 && e.id === void 0 && e.key === void 0 ? (q(), J("optgroup", {
				key: 0,
				label: e.label || e.title || "Options"
			}, [(q(!0), J(K, null, G(e.options, (e) => (q(), J("option", {
				key: N(e),
				value: N(e)
			}, V(P(e)), 9, Xf))), 128))], 8, Yf)) : (q(), J("option", {
				key: 1,
				value: N(e)
			}, V(P(e)), 9, Zf))], 64))), 128))], 40, Jf),
			e.field.description ? (q(), J("small", Qf, V(e.field.description), 1)) : Z("", !0)
		])) : d.value === "textarea" || d.value === "multiline" ? (q(), J("label", $f, [
			e.field.hide_label ? Z("", !0) : (q(), J("span", ep, V(p.value), 1)),
			y.value ? (q(), J("div", tp, V(m.value), 1)) : (q(), J("textarea", {
				key: 2,
				value: m.value,
				rows: e.field.rows || 4,
				placeholder: e.field.placeholder,
				disabled: v.value,
				onInput: ee
			}, null, 40, np)),
			e.field.description ? (q(), J("small", rp, V(e.field.description), 1)) : Z("", !0)
		])) : d.value === "file" ? (q(), J("label", ip, [
			Y("span", null, V(p.value), 1),
			Y("input", {
				type: "file",
				accept: e.field.accept,
				capture: e.field.camera_capture ? e.field.camera_facing_mode === "environment" ? "environment" : "user" : void 0,
				disabled: v.value || y.value,
				onChange: ie
			}, null, 40, ap),
			e.field.camera_capture ? (q(), J("div", op, [i.value ? (q(), J(K, { key: 1 }, [Y("button", {
				class: "action-btn",
				type: "button",
				onClick: se
			}, "Take photo"), Y("button", {
				class: "inline-btn",
				type: "button",
				onClick: oe
			}, "Cancel")], 64)) : (q(), J("button", {
				key: 0,
				class: "inline-btn",
				type: "button",
				onClick: ae
			}, "Use camera"))])) : Z("", !0),
			i.value ? (q(), J("video", {
				key: 1,
				ref_key: "cameraVideo",
				ref: a,
				class: "tcx-native-camera",
				autoplay: "",
				muted: "",
				playsinline: ""
			}, null, 512)) : Z("", !0),
			Y("small", null, V(e.modelValue ? "A value is ready. Choose another file to replace it." : "No file selected."), 1),
			e.field.description ? (q(), J("small", sp, V(e.field.description), 1)) : Z("", !0)
		])) : (q(), J("label", cp, [
			e.field.hide_label ? Z("", !0) : (q(), J("span", lp, V(p.value), 1)),
			y.value ? (q(), J("div", up, V(m.value), 1)) : (q(), J("input", {
				key: 2,
				type: d.value === "password" ? "password" : d.value === "number" ? "number" : "text",
				value: e.modelValue,
				min: e.field.min,
				max: e.field.max,
				step: e.field.step,
				placeholder: e.field.placeholder,
				disabled: v.value,
				onInput: ee
			}, null, 40, dp)),
			_.value && e.field.disabled_note ? (q(), J("small", fp, V(e.field.disabled_note), 1)) : Z("", !0),
			e.field.description ? (q(), J("small", pp, V(e.field.description), 1)) : Z("", !0)
		]))], 2)) : Z("", !0);
	}
}), hp = ["data-core-item-group", "tabindex"], gp = { class: "card-head" }, _p = { class: "card-title" }, vp = { class: "core-manager-card-tools" }, yp = {
	key: 0,
	class: "core-manager-item-selection"
}, bp = ["checked", "aria-label"], xp = { class: "small" }, Sp = {
	key: 0,
	class: "core-satellite-image-wrap"
}, Cp = ["src", "alt"], wp = { class: "core-satellite-summary-main" }, Tp = {
	key: 0,
	class: "small core-satellite-subtitle"
}, Ep = {
	key: 1,
	class: "core-satellite-badges"
}, Dp = {
	key: 2,
	class: "small core-satellite-detail"
}, Op = {
	key: 3,
	class: "core-satellite-facts"
}, kp = { class: "small core-satellite-fact-label" }, Ap = { class: "core-satellite-fact-value" }, jp = {
	key: 4,
	class: "core-satellite-sensors"
}, Mp = { class: "small core-satellite-sensors-title" }, Np = { class: "core-satellite-sensor-grid" }, Pp = { class: "core-satellite-sensor-label" }, Fp = { class: "core-satellite-sensor-value" }, Ip = {
	key: 0,
	class: "core-satellite-sensor-meta"
}, Lp = {
	key: 1,
	class: "small"
}, Rp = ["open"], zp = { class: "settings-summary" }, Bp = {
	key: 0,
	class: "form-grid tcx-native-fields"
}, Vp = { class: "small core-inline-section-title" }, Hp = { class: "form-grid tcx-native-fields" }, Up = {
	key: 3,
	class: "form-grid tcx-native-fields"
}, Wp = { class: "settings-summary" }, Gp = { class: "form-grid tcx-native-fields" }, Kp = { class: "small core-inline-section-title" }, qp = { class: "form-grid tcx-native-fields" }, Jp = {
	key: 5,
	class: "inline-row tcx-native-actions"
}, Yp = [
	"disabled",
	"title",
	"onClick"
], Xp = ["disabled"], Zp = ["disabled"], Qp = ["disabled"], $p = ["disabled"], em = { class: "small core-manager-status" }, tm = { class: "tv-eyebrow" }, nm = { class: "form-grid tcx-native-fields" }, rm = { class: "inline-row" }, im = ["disabled"], am = /* @__PURE__ */ sr({
	__name: "CoreManagerItem",
	props: {
		item: {},
		ui: {},
		run: { type: Function },
		busy: { type: Function },
		selected: { type: Boolean }
	},
	emits: ["select"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ Et({}), a = /* @__PURE__ */ new Set(), o = /* @__PURE__ */ U(""), s = /* @__PURE__ */ U(!1), c = /* @__PURE__ */ U(!1), l = Q(() => D(n.item.id)), u = Q(() => D(n.item.title || n.item.id) || "(item)"), d = Q(() => Array.isArray(n.item.fields) ? n.item.fields : []), f = Q(() => Array.isArray(n.item.sections) ? n.item.sections : []), p = Q(() => Array.isArray(n.item.actions) ? n.item.actions.filter((e) => D(e.action)) : []), m = Q(() => !!n.ui.item_fields_popup && n.item.fields_popup !== !1), h = Q(() => !!(n.item.fields_dropdown ?? n.ui.item_fields_dropdown)), g = Q(() => !!(n.item.sections_in_dropdown ?? n.ui.item_sections_in_dropdown)), _ = Q(() => {
			let e = (Array.isArray(n.item.popup_fields) ? n.item.popup_fields : []).map((e) => ({ ...e }));
			return m.value && (e.push(...d.value.map((e) => ({ ...e }))), f.value.forEach((t) => (Array.isArray(t.fields) ? t.fields : []).forEach((n) => e.push({
				...n,
				label: `${D(t.label) || "Section"} • ${D(n.label || n.key) || "Field"}`
			})))), e;
		}), v = Q(() => _.value.length > 0 && (m.value || Array.isArray(n.item.popup_fields))), y = Q(() => f.value.filter((e) => g.value || e.inline)), b = Q(() => f.value.filter((e) => !g.value && !e.inline)), x = Q(() => Array.isArray(n.item.summary_rows) ? n.item.summary_rows : []), S = Q(() => Array.isArray(n.item.sensor_rows) ? n.item.sensor_rows : []), C = Q(() => Array.isArray(n.item.hero_badges) ? n.item.hero_badges : []), w = Q(() => !!(n.item.hero_image_src || C.value.length || x.value.length || S.value.length || n.item.detail)), T = Q(() => [D(n.item.group) ? `core-manager-item-${O(n.item.group)}` : "", D(n.item.card_variant) ? `core-manager-item-variant-${O(n.item.card_variant)}` : ""]), E = Q(() => [
			...d.value,
			...f.value.flatMap((e) => Array.isArray(e.fields) ? e.fields : []),
			...Array.isArray(n.item.popup_fields) ? n.item.popup_fields : []
		]);
		function D(e) {
			return String(e ?? "").trim();
		}
		function O(e) {
			return D(e).toLowerCase().replace(/[^a-z0-9_-]/g, "");
		}
		function k(e) {
			return Array.isArray(e) ? e.map(k) : e && typeof e == "object" ? { ...e } : e;
		}
		function A(e) {
			return e.value === void 0 ? e.default === void 0 ? D(e.type).toLowerCase() === "checkbox" ? !1 : [
				"multiselect",
				"multi_choice_cards",
				"image_checklist"
			].includes(D(e.type).toLowerCase()) ? [] : "" : k(e.default) : k(e.value);
		}
		function j() {
			E.value.forEach((e) => {
				let t = D(e.key);
				t && !a.has(t) && (i[t] = A(e));
			});
		}
		function M(e, t) {
			let n = D(e.key);
			n && (i[n] = t, a.add(n));
		}
		function N(e) {
			return `item:${l.value}:${e}`;
		}
		async function P(e, t, r = "Done.") {
			let i = D(e);
			if (!i) return null;
			o.value = "Working…";
			let s = await n.run(i, t, N(i), r);
			return s ? (o.value = D(s.message || r), a.clear()) : o.value = "", s;
		}
		async function F(e = !1) {
			n.item.save_action && await P(n.item.save_action, {
				id: l.value,
				values: { ...i }
			}, D(n.item.save_success_text) || "Saved.") && e && (s.value = !1);
		}
		async function I() {
			let e = D(n.item.reset_action);
			if (!e) return;
			let t = D(n.item.reset_confirm) || "Reset this item to defaults?";
			t && !window.confirm(t) || await P(e, { id: l.value }, "Defaults restored.");
		}
		async function ee() {
			let e = D(n.item.remove_action);
			!e || !window.confirm(D(n.item.remove_confirm) || "Remove this item?") || await P(e, { id: l.value }, "Removed.");
		}
		async function te() {
			let e = D(n.item.run_action), t = D(n.item.run_confirm);
			!e || t && !window.confirm(t) || await P(e, {
				id: l.value,
				values: { ...i }
			}, "Queued.");
		}
		async function ne(e) {
			let t = D(e.action), n = D(e.confirm);
			!t || n && !window.confirm(n) || await P(t, {
				id: l.value,
				values: { ...i }
			}, D(e.success_text) || "Done.");
		}
		async function L(e) {
			let t = D(e.action);
			t && await P(t, {
				id: l.value,
				values: { ...i }
			}, "Updated.");
		}
		function R(e) {
			n.item.click_opens_fields && (e.target?.closest("button, input, select, textarea, a, label, summary, details") || (c.value = !c.value));
		}
		function z(e) {
			(e.key === "Enter" || e.key === " ") && e.target === e.currentTarget && (e.preventDefault(), R(e));
		}
		return On(E, j, { immediate: !0 }), (t, n) => (q(), J(K, null, [Y("article", {
			class: B(["card core-manager-item tcx-native-item", T.value]),
			"data-core-item-group": e.item.group || "",
			tabindex: e.item.click_opens_fields ? 0 : void 0,
			onClick: R,
			onKeydown: z
		}, [
			Y("div", gp, [Y("h3", _p, V(u.value), 1), Y("div", vp, [e.item.selectable ? (q(), J("label", yp, [Y("input", {
				class: "toggle-input",
				type: "checkbox",
				checked: e.selected,
				"aria-label": e.item.selection_label || `Select ${u.value}`,
				onChange: n[0] ||= (e) => r("select", e.target.checked)
			}, null, 40, bp), n[13] ||= Y("span", null, "Select", -1)])) : Z("", !0), Y("span", xp, V(e.item.core_key), 1)])]),
			w.value ? (q(), J("div", {
				key: 0,
				class: B(["core-satellite-summary", { "no-image": !e.item.hero_image_src }])
			}, [e.item.hero_image_src ? (q(), J("div", Sp, [Y("img", {
				class: "core-satellite-image",
				src: e.item.hero_image_src,
				alt: e.item.hero_image_alt || u.value
			}, null, 8, Cp)])) : Z("", !0), Y("div", wp, [
				e.item.subtitle ? (q(), J("div", Tp, V(e.item.subtitle), 1)) : Z("", !0),
				C.value.length ? (q(), J("div", Ep, [(q(!0), J(K, null, G(C.value, (e) => (q(), J("span", {
					key: e.label,
					class: B(["core-satellite-badge", `tone-${O(e.tone || "muted")}`])
				}, V(e.label), 3))), 128))])) : Z("", !0),
				e.item.detail ? (q(), J("div", Dp, V(e.item.detail), 1)) : Z("", !0),
				x.value.length ? (q(), J("div", Op, [(q(!0), J(K, null, G(x.value, (e) => (q(), J("div", {
					key: e.label,
					class: "core-satellite-fact"
				}, [Y("div", kp, V(e.label), 1), Y("div", Ap, V(e.value), 1)]))), 128))])) : Z("", !0),
				S.value.length ? (q(), J("div", jp, [Y("div", Mp, V(e.item.sensor_title || "Sensors"), 1), Y("div", Np, [(q(!0), J(K, null, G(S.value, (e) => (q(), J("div", {
					key: e.label,
					class: "core-satellite-sensor-pill"
				}, [
					Y("span", Pp, V(e.label), 1),
					Y("span", Fp, V(e.value), 1),
					e.meta || e.kind ? (q(), J("span", Ip, V(e.meta || e.kind), 1)) : Z("", !0)
				]))), 128))])])) : Z("", !0)
			])], 2)) : e.item.subtitle ? (q(), J("div", Lp, V(e.item.subtitle), 1)) : Z("", !0),
			h.value && (d.value.length || y.value.length) ? (q(), J("details", {
				key: 2,
				class: "settings-dropdown",
				open: c.value,
				onToggle: n[3] ||= (e) => c.value = e.target.open
			}, [
				Y("summary", zp, V(e.item.fields_dropdown_label || e.ui.item_fields_dropdown_label || "Settings"), 1),
				d.value.length ? (q(), J("div", Bp, [(q(!0), J(K, null, G(d.value, (e) => (q(), da(mp, {
					key: e.key,
					field: e,
					"model-value": i[e.key],
					"all-values": i,
					"onUpdate:modelValue": (t) => M(e, t),
					onCommit: (t) => L(e),
					onError: n[1] ||= (e) => o.value = e
				}, null, 8, [
					"field",
					"model-value",
					"all-values",
					"onUpdate:modelValue",
					"onCommit"
				]))), 128))])) : Z("", !0),
				(q(!0), J(K, null, G(y.value, (e) => (q(), J("section", {
					key: e.label,
					class: B(["core-inline-section", e.tone ? `tone-${O(e.tone)}` : ""])
				}, [Y("div", Vp, V(e.label || "Section"), 1), Y("div", Hp, [(q(!0), J(K, null, G(e.fields || [], (e) => (q(), da(mp, {
					key: e.key,
					field: e,
					"model-value": i[e.key],
					"all-values": i,
					"onUpdate:modelValue": (t) => M(e, t),
					onCommit: (t) => L(e),
					onError: n[2] ||= (e) => o.value = e
				}, null, 8, [
					"field",
					"model-value",
					"all-values",
					"onUpdate:modelValue",
					"onCommit"
				]))), 128))])], 2))), 128))
			], 40, Rp)) : !m.value && d.value.length ? (q(), J("div", Up, [(q(!0), J(K, null, G(d.value, (e) => (q(), da(mp, {
				key: e.key,
				field: e,
				"model-value": i[e.key],
				"all-values": i,
				"onUpdate:modelValue": (t) => M(e, t),
				onCommit: (t) => L(e),
				onError: n[4] ||= (e) => o.value = e
			}, null, 8, [
				"field",
				"model-value",
				"all-values",
				"onUpdate:modelValue",
				"onCommit"
			]))), 128))])) : Z("", !0),
			!m.value && !h.value && !g.value ? (q(), J(K, { key: 4 }, [(q(!0), J(K, null, G(b.value, (e) => (q(), J("details", {
				key: e.label,
				class: "settings-dropdown"
			}, [Y("summary", Wp, V(e.label || "Section"), 1), Y("div", Gp, [(q(!0), J(K, null, G(e.fields || [], (e) => (q(), da(mp, {
				key: e.key,
				field: e,
				"model-value": i[e.key],
				"all-values": i,
				"onUpdate:modelValue": (t) => M(e, t),
				onCommit: (t) => L(e),
				onError: n[5] ||= (e) => o.value = e
			}, null, 8, [
				"field",
				"model-value",
				"all-values",
				"onUpdate:modelValue",
				"onCommit"
			]))), 128))])]))), 128)), (q(!0), J(K, null, G(y.value, (e) => (q(), J("section", {
				key: `inline-${e.label}`,
				class: B(["core-inline-section", e.tone ? `tone-${O(e.tone)}` : ""])
			}, [Y("div", Kp, V(e.label || "Section"), 1), Y("div", qp, [(q(!0), J(K, null, G(e.fields || [], (e) => (q(), da(mp, {
				key: e.key,
				field: e,
				"model-value": i[e.key],
				"all-values": i,
				"onUpdate:modelValue": (t) => M(e, t),
				onCommit: (t) => L(e),
				onError: n[6] ||= (e) => o.value = e
			}, null, 8, [
				"field",
				"model-value",
				"all-values",
				"onUpdate:modelValue",
				"onCommit"
			]))), 128))])], 2))), 128))], 64)) : Z("", !0),
			p.value.length || e.item.save_action || e.item.reset_action || e.item.remove_action || e.item.run_action || v.value ? (q(), J("div", Jp, [
				(q(!0), J(K, null, G(p.value, (t) => (q(), J("button", {
					key: t.action,
					type: "button",
					class: B(t.tone === "danger" ? "inline-btn danger" : t.tone === "muted" ? "inline-btn" : "action-btn"),
					disabled: e.busy(N(t.action)),
					title: t.tooltip || t.title,
					onClick: (e) => ne(t)
				}, V(t.label || t.title || "Run"), 11, Yp))), 128)),
				e.item.save_action && e.item.show_save_button !== !1 && !m.value ? (q(), J("button", {
					key: 0,
					type: "button",
					class: "action-btn",
					disabled: e.busy(N(e.item.save_action)),
					onClick: n[7] ||= (e) => F(!1)
				}, V(e.item.save_label || "Save"), 9, Xp)) : Z("", !0),
				e.item.reset_action ? (q(), J("button", {
					key: 1,
					type: "button",
					class: "inline-btn danger",
					disabled: e.busy(N(e.item.reset_action)),
					onClick: I
				}, V(e.item.reset_label || "Restore Default Settings"), 9, Zp)) : Z("", !0),
				e.item.remove_action ? (q(), J("button", {
					key: 2,
					type: "button",
					class: "inline-btn danger",
					disabled: e.busy(N(e.item.remove_action)),
					onClick: ee
				}, V(e.item.remove_label || "Remove"), 9, Qp)) : Z("", !0),
				v.value ? (q(), J("button", {
					key: 3,
					type: "button",
					class: "action-btn",
					onClick: n[8] ||= (e) => s.value = !0
				}, V(e.item.settings_label || e.ui.item_fields_popup_label || "Settings"), 1)) : Z("", !0),
				e.item.run_action ? (q(), J("button", {
					key: 4,
					type: "button",
					class: "action-btn tcx-run-action",
					disabled: e.busy(N(e.item.run_action)),
					onClick: te
				}, V(e.item.run_label || "Run Now"), 9, $p)) : Z("", !0),
				Y("span", em, V(o.value), 1)
			])) : Z("", !0)
		], 42, hp), ga(kl, {
			open: s.value,
			onClose: n[12] ||= (e) => s.value = !1
		}, {
			default: Sn(() => [Y("form", {
				class: "tv-modal tcx-native-popup",
				onSubmit: n[11] ||= bs((e) => F(!0), ["prevent"])
			}, [
				Y("header", null, [Y("div", null, [Y("span", tm, V(e.item.group || "Core item"), 1), Y("h2", null, V(e.item.settings_title || `${u.value} Settings`), 1)]), Y("button", {
					class: "tv-button",
					type: "button",
					onClick: n[9] ||= (e) => s.value = !1
				}, "Close")]),
				Y("div", nm, [(q(!0), J(K, null, G(_.value, (e) => (q(), da(mp, {
					key: e.key || e.label,
					field: e,
					"model-value": i[e.key],
					"all-values": i,
					"onUpdate:modelValue": (t) => M(e, t),
					onCommit: (t) => L(e),
					onError: n[10] ||= (e) => o.value = e
				}, null, 8, [
					"field",
					"model-value",
					"all-values",
					"onUpdate:modelValue",
					"onCommit"
				]))), 128))]),
				Y("footer", null, [Y("span", null, V(o.value || "Ready"), 1), Y("div", rm, [e.item.reset_action ? (q(), J("button", {
					key: 0,
					class: "tv-button danger",
					type: "button",
					onClick: I
				}, V(e.item.reset_label || "Restore defaults"), 1)) : Z("", !0), e.item.save_action ? (q(), J("button", {
					key: 1,
					class: "tv-button primary",
					type: "submit",
					disabled: e.busy(N(e.item.save_action))
				}, V(e.item.save_label || "Save"), 9, im)) : Z("", !0)])])
			], 32)]),
			_: 1
		}, 8, ["open"])], 64));
	}
}), om = {
	key: 0,
	class: "tcx-native-items-wrap"
}, sm = {
	key: 0,
	class: "core-manager-bulk-toolbar"
}, cm = { class: "core-manager-bulk-select-all" }, lm = ["checked"], um = { class: "small" }, dm = { class: "core-manager-bulk-buttons" }, fm = ["disabled", "onClick"], pm = {
	key: 1,
	class: "tcx-native-selector"
}, mm = ["value"], hm = {
	key: 2,
	class: "inline-row tcx-native-pagination"
}, gm = ["disabled"], _m = { class: "small" }, vm = ["disabled"], ym = {
	key: 1,
	class: "tv-empty compact"
}, bm = /* @__PURE__ */ sr({
	__name: "CoreManagerItems",
	props: {
		items: {},
		ui: {},
		options: {},
		run: { type: Function },
		busy: { type: Function }
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ U(1), r = /* @__PURE__ */ U(""), i = /* @__PURE__ */ U([]), a = Q(() => Array.isArray(t.items) ? t.items : []), o = Q(() => !!t.options?.selector), s = Q(() => Math.max(0, Math.floor(Number(t.options?.page_size || 0)))), c = Q(() => s.value > 0 ? Math.max(1, Math.ceil(a.value.length / s.value)) : 1), l = Q(() => t.options?.server_pagination && typeof t.options.server_pagination == "object" && t.options.server_pagination.enabled ? t.options.server_pagination : null), u = Q(() => l.value ? Math.max(1, Number(l.value.page || 1)) : n.value), d = Q(() => l.value ? Math.max(1, Number(l.value.page_count || 1)) : c.value), f = Q(() => {
			if (o.value) {
				let e = r.value || _(a.value[0]);
				return a.value.filter((t) => _(t) === e).slice(0, 1);
			}
			if (l.value || s.value <= 0) return a.value;
			let e = (n.value - 1) * s.value;
			return a.value.slice(e, e + s.value);
		}), p = Q(() => a.value.filter((e) => e.selectable).map(_).filter(Boolean)), m = Q(() => !!p.value.length && p.value.every((e) => i.value.includes(e))), h = Q(() => Array.isArray(t.options?.bulk_actions) ? t.options.bulk_actions.filter((e) => e.action) : []);
		function g(e) {
			return String(e ?? "").trim();
		}
		function _(e) {
			return g(e?.id);
		}
		function v(e, t) {
			let n = new Set(i.value);
			t ? n.add(e) : n.delete(e), i.value = [...n];
		}
		function y(e) {
			i.value = e ? [...p.value] : [];
		}
		async function b(e) {
			let n = Math.max(1, Number(e.minimum_selected || 1));
			if (i.value.length < n) return;
			let r = g(e.confirm);
			r && !window.confirm(r) || await t.run(g(e.action), {
				ids: [...i.value],
				values: { identity_ids: [...i.value] }
			}, `bulk:${e.action}`, g(e.success_text) || "Done.") && (i.value = []);
		}
		async function x(e) {
			let r = Math.min(d.value, Math.max(1, u.value + e));
			if (r !== u.value) if (l.value) {
				let e = g(l.value.action);
				if (!e) return;
				await t.run(e, {
					page: r,
					page_size: Math.max(1, Number(l.value.page_size || s.value || 1))
				}, `page:${e}`, `Loaded page ${r}.`);
			} else n.value = r;
		}
		return On(a, (e) => {
			let t = new Set(e.map(_));
			i.value = i.value.filter((e) => t.has(e)), t.has(r.value) || (r.value = _(e[0])), n.value > c.value && (n.value = c.value);
		}, { immediate: !0 }), (t, n) => a.value.length ? (q(), J("div", om, [
			h.value.length ? (q(), J("div", sm, [
				Y("label", cm, [Y("input", {
					class: "toggle-input",
					type: "checkbox",
					checked: m.value,
					onChange: n[0] ||= (e) => y(e.target.checked)
				}, null, 40, lm), n[4] ||= Y("span", null, "Select all", -1)]),
				Y("span", um, V(i.value.length) + " selected", 1),
				Y("div", dm, [(q(!0), J(K, null, G(h.value, (t) => (q(), J("button", {
					key: t.action,
					type: "button",
					class: B(t.tone === "danger" ? "inline-btn danger" : "action-btn"),
					disabled: i.value.length < Math.max(1, Number(t.minimum_selected || 1)) || e.busy(`bulk:${t.action}`),
					onClick: (e) => b(t)
				}, V(t.label || "Apply to selected"), 11, fm))), 128))])
			])) : Z("", !0),
			o.value && a.value.length > 1 ? (q(), J("label", pm, [X(V(e.options?.selector_label || "Select Item"), 1), W(Y("select", { "onUpdate:modelValue": n[1] ||= (e) => r.value = e }, [(q(!0), J(K, null, G(a.value, (e) => (q(), J("option", {
				key: _(e),
				value: _(e)
			}, V(e.title || e.id), 9, mm))), 128))], 512), [[ds, r.value]])])) : Z("", !0),
			Y("div", { class: B(["core-tab-items", e.options?.item_group ? `core-tab-items-group-${String(e.options.item_group).toLowerCase().replace(/[^a-z0-9_-]/g, "")}` : ""]) }, [(q(!0), J(K, null, G(f.value, (t) => (q(), da(am, {
				key: _(t),
				item: t,
				ui: e.ui,
				run: e.run,
				busy: e.busy,
				selected: i.value.includes(_(t)),
				onSelect: (e) => v(_(t), e)
			}, null, 8, [
				"item",
				"ui",
				"run",
				"busy",
				"selected",
				"onSelect"
			]))), 128))], 2),
			d.value > 1 ? (q(), J("div", hm, [
				Y("button", {
					class: "action-btn",
					type: "button",
					disabled: u.value <= 1 || e.busy(`page:${l.value?.action || ""}`),
					onClick: n[2] ||= (e) => x(-1)
				}, "Previous", 8, gm),
				Y("span", _m, "Page " + V(u.value) + " of " + V(d.value), 1),
				Y("button", {
					class: "action-btn",
					type: "button",
					disabled: u.value >= d.value || e.busy(`page:${l.value?.action || ""}`),
					onClick: n[3] ||= (e) => x(1)
				}, "Next", 8, vm)
			])) : Z("", !0)
		])) : (q(), J("div", ym, V(e.options?.empty_message || e.ui.empty_message || "No entries found."), 1));
	}
}), xm = {
	class: "tcx-native-panel",
	"data-core-renderer": "vue"
}, Sm = {
	key: 0,
	class: "card"
}, Cm = { class: "card-head" }, wm = { class: "card-title" }, Tm = { class: "small" }, Em = { class: "tv-notice error" }, Dm = ["data-core-live-updates"], Om = { class: "card-head" }, km = { class: "card-title" }, Am = { class: "small" }, jm = {
	key: 0,
	class: "small"
}, Mm = {
	key: 3,
	class: "core-metric-row tcx-native-stats"
}, Nm = { class: "small" }, Pm = ["disabled"], Fm = { class: "card tcx-native-manager-body" }, Im = { class: "card-head" }, Lm = { class: "card-title" }, Rm = {
	key: 0,
	class: "core-manager-tabs",
	"aria-label": "Core manager sections"
}, zm = ["onClick"], Bm = { class: "inline-row tcx-native-form-actions" }, Vm = ["disabled"], Hm = {
	key: 1,
	class: "core-manager-subtabs-wrap"
}, Um = {
	class: "core-manager-subtabs",
	"aria-label": "Core item groups"
}, Wm = ["onClick"], Gm = { class: "inline-row tcx-native-form-actions" }, Km = ["disabled"], qm = {
	key: 2,
	class: "card tcx-native-simple"
}, Jm = { class: "card-head" }, Ym = { class: "card-title" }, Xm = { class: "small" }, Zm = {
	key: 0,
	class: "small"
}, Qm = {
	key: 1,
	class: "core-metric-row"
}, $m = { class: "small" }, eh = {
	key: 2,
	class: "core-tab-items"
}, th = { class: "card-head" }, nh = { class: "card-title" }, rh = {
	key: 0,
	class: "small"
}, ih = {
	key: 1,
	class: "muted"
}, ah = {
	key: 3,
	class: "tv-empty compact"
}, oh = /* @__PURE__ */ sr({
	__name: "CorePanelRenderer",
	props: {
		payload: {},
		tab: {},
		actionEndpoint: {},
		refresh: { type: Function },
		notify: { type: Function }
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ U(""), r = /* @__PURE__ */ U(""), i = /* @__PURE__ */ U(""), a = /* @__PURE__ */ U(""), o = /* @__PURE__ */ U([]), s = /* @__PURE__ */ Et({}), c = /* @__PURE__ */ Et({}), l = /* @__PURE__ */ new Set(), u = Q(() => t.payload && typeof t.payload == "object" ? t.payload : {}), d = Q(() => u.value.ui && typeof u.value.ui == "object" ? u.value.ui : {}), f = Q(() => w(d.value.kind) === "settings_manager"), p = Q(() => Array.isArray(u.value.stats) ? u.value.stats : []), m = Q(() => Array.isArray(d.value.item_forms) ? d.value.item_forms : []), h = Q(() => d.value.add_form && typeof d.value.add_form == "object" ? d.value.add_form : {}), g = Q(() => Array.isArray(h.value.fields) ? h.value.fields : []), _ = Q(() => (Array.isArray(d.value.manager_tabs) ? d.value.manager_tabs : []).filter((e) => w(e.key))), v = Q(() => _.value.find((e) => w(e.key) === n.value) || _.value[0] || null), y = Q(() => v.value && Array.isArray(v.value.groups) ? v.value.groups.filter((e) => w(e.key)) : []), b = Q(() => y.value.find((e) => w(e.key) === r.value) || y.value[0] || null), x = Q(() => {
			let e = new Set((Array.isArray(d.value.persistent_item_groups) ? d.value.persistent_item_groups : []).map((e) => w(e).toLowerCase()).filter(Boolean));
			return m.value.filter((t) => e.has(w(t.group).toLowerCase()));
		}), S = Q(() => Array.isArray(d.value.stats_controls) ? d.value.stats_controls : []), C = Q(() => w(d.value.appearance) ? `core-settings-manager-${T(d.value.appearance)}` : "");
		function w(e) {
			return String(e ?? "").trim();
		}
		function T(e) {
			return w(e).toLowerCase().replace(/[^a-z0-9_-]/g, "");
		}
		function E(e) {
			return Array.isArray(e) ? e.map(E) : e && typeof e == "object" ? { ...e } : e;
		}
		function D(e) {
			return e.value === void 0 ? e.default === void 0 ? w(e.type).toLowerCase() === "checkbox" ? !1 : [
				"multiselect",
				"multi_choice_cards",
				"image_checklist"
			].includes(w(e.type).toLowerCase()) ? [] : "" : E(e.default) : E(e.value);
		}
		function O(e) {
			return o.value.includes(e);
		}
		function k(e, t) {
			let n = new Set(o.value);
			t ? n.add(e) : n.delete(e), o.value = [...n];
		}
		async function A(e, n, r = e, o = "Done.") {
			let s = w(e);
			if (!s || O(r)) return null;
			k(r, !0), a.value = "", i.value = "Working…";
			try {
				let e = await js(t.actionEndpoint, {
					action: s,
					payload: n || {}
				}), r = w(e?.message) || o;
				i.value = r;
				let a = w(e?.sample_url);
				if (a) try {
					await new Audio(a).play();
				} catch {
					t.notify?.("The sample is ready, but the browser could not play it.", "error");
				}
				return await t.refresh(), t.notify?.(r, "success"), e || {};
			} catch (e) {
				let n = e instanceof Error ? e.message : "Core action failed.";
				return a.value = n, i.value = "", t.notify?.(n, "error"), null;
			} finally {
				k(r, !1);
			}
		}
		function j(e) {
			return e.map((e) => ({
				...e,
				core_key: t.tab.core_key
			}));
		}
		function M(e) {
			let t = w(e).toLowerCase();
			return j(t ? m.value.filter((e) => w(e.group).toLowerCase() === t) : m.value);
		}
		function N(e) {
			return e ? {
				selector: !!e.selector,
				selector_label: e.selector_label,
				item_group: e.item_group,
				page_size: e.page_size,
				server_pagination: e.server_pagination,
				bulk_actions: e.bulk_actions,
				empty_message: e.empty_message || u.value.empty_message
			} : {};
		}
		function P(e, t) {
			let n = w(e.key);
			n && (s[n] = t, l.add(n));
		}
		async function F() {
			let e = w(h.value.action);
			e && await A(e, {
				...s,
				values: { ...s }
			}, `add:${e}`, w(h.value.success_text) || "Saved.") && (l.clear(), g.value.forEach((e) => {
				s[w(e.key)] = D(e);
			}));
		}
		async function I(e, t) {
			let n = w(e.key);
			n && (c[n] = t, d.value.stats_controls_auto_save !== !1 && d.value.stats_controls_action && await A(w(d.value.stats_controls_action), {
				...c,
				values: { ...c }
			}, "stats-controls", "Saved."));
		}
		async function ee() {
			d.value.stats_controls_action && await A(w(d.value.stats_controls_action), {
				...c,
				values: { ...c }
			}, "stats-controls", "Saved.");
		}
		return On(_, (e) => {
			let t = new Set(e.map((e) => w(e.key))), r = w(d.value.default_tab);
			t.has(n.value) || (n.value = t.has(r) ? r : w(e[0]?.key));
		}, { immediate: !0 }), On(y, (e) => {
			new Set(e.map((e) => w(e.key))).has(r.value) || (r.value = w(e[0]?.key));
		}, { immediate: !0 }), On(g, (e) => e.forEach((e) => {
			let t = w(e.key);
			t && !l.has(t) && (s[t] = D(e));
		}), { immediate: !0 }), On(S, (e) => e.forEach((e) => {
			let t = w(e.key);
			t && (c[t] = D(e));
		}), { immediate: !0 }), (t, o) => (q(), J("div", xm, [u.value.error ? (q(), J("div", Sm, [Y("div", Cm, [Y("h3", wm, V(e.tab.label || e.tab.core_key), 1), Y("span", Tm, V(e.tab.core_key), 1)]), Y("div", Em, V(u.value.error), 1)])) : f.value ? (q(), J("div", {
			key: 1,
			class: B(["card core-settings-manager tcx-native-manager", C.value]),
			"data-core-live-updates": d.value.live_updates ? "1" : "0"
		}, [
			Y("div", Om, [Y("h3", km, V(e.tab.label || e.tab.core_key), 1), Y("span", Am, V(e.tab.core_key), 1)]),
			u.value.summary ? (q(), J("div", jm, V(u.value.summary), 1)) : Z("", !0),
			a.value || i.value ? (q(), J("div", {
				key: 1,
				class: B(["tv-notice compact", { error: !!a.value }])
			}, V(a.value || i.value), 3)) : Z("", !0),
			x.value.length ? (q(), da(bm, {
				key: 2,
				class: "core-manager-persistent",
				items: j(x.value),
				ui: d.value,
				options: { empty_message: u.value.empty_message },
				run: A,
				busy: O
			}, null, 8, [
				"items",
				"ui",
				"options"
			])) : Z("", !0),
			p.value.length || S.value.length || d.value.stats_refresh_button ? (q(), J("div", Mm, [
				(q(!0), J(K, null, G(p.value, (e) => (q(), J("div", {
					key: e.label,
					class: "core-metric-pill"
				}, [Y("div", Nm, V(e.label), 1), Y("div", null, V(e.value ?? "-"), 1)]))), 128)),
				S.value.length && d.value.stats_controls_action ? (q(), J("form", {
					key: 0,
					class: "inline-row tcx-native-stats-controls",
					onSubmit: bs(ee, ["prevent"])
				}, [(q(!0), J(K, null, G(S.value, (e) => (q(), da(mp, {
					key: e.key,
					field: e,
					"model-value": c[e.key],
					"all-values": c,
					"onUpdate:modelValue": (t) => I(e, t),
					onError: o[0] ||= (e) => a.value = e
				}, null, 8, [
					"field",
					"model-value",
					"all-values",
					"onUpdate:modelValue"
				]))), 128)), d.value.stats_controls_auto_save === !1 ? (q(), J("button", {
					key: 0,
					class: "action-btn",
					type: "submit",
					disabled: O("stats-controls")
				}, "Save", 8, Pm)) : Z("", !0)], 32)) : Z("", !0),
				d.value.stats_refresh_button ? (q(), J("button", {
					key: 1,
					class: "action-btn",
					type: "button",
					onClick: o[1] ||= (...t) => e.refresh && e.refresh(...t)
				}, V(d.value.stats_refresh_label || "Refresh"), 1)) : Z("", !0)
			])) : Z("", !0),
			Y("div", Fm, [
				Y("div", Im, [Y("h3", Lm, V(d.value.title || "Manager"), 1)]),
				_.value.length ? (q(), J("nav", Rm, [(q(!0), J(K, null, G(_.value, (e) => (q(), J("button", {
					key: e.key,
					type: "button",
					class: B(["core-manager-tab-btn", { active: n.value === e.key }]),
					onClick: (t) => n.value = e.key
				}, V(e.label || e.key), 11, zm))), 128))])) : Z("", !0),
				_.value.length ? (q(), J(K, { key: 1 }, [v.value?.source === "add_form" ? (q(), J("form", {
					key: 0,
					class: "form-grid core-manager-add-form tcx-native-add-form",
					onSubmit: bs(F, ["prevent"])
				}, [(q(!0), J(K, null, G(g.value, (e) => (q(), da(mp, {
					key: e.key,
					field: e,
					"model-value": s[e.key],
					"all-values": s,
					"onUpdate:modelValue": (t) => P(e, t),
					onError: o[2] ||= (e) => a.value = e
				}, null, 8, [
					"field",
					"model-value",
					"all-values",
					"onUpdate:modelValue"
				]))), 128)), Y("div", Bm, [Y("button", {
					class: "action-btn",
					type: "submit",
					disabled: O(`add:${h.value.action}`)
				}, V(h.value.submit_label || "Add"), 9, Vm)])], 32)) : v.value?.source === "grouped_items" && y.value.length ? (q(), J("div", Hm, [Y("nav", Um, [(q(!0), J(K, null, G(y.value, (e) => (q(), J("button", {
					key: e.key,
					type: "button",
					class: B(["core-manager-subtab-btn", { active: r.value === e.key }]),
					onClick: (t) => r.value = e.key
				}, V(e.label || e.key), 11, Wm))), 128))]), b.value ? (q(), da(bm, {
					key: 0,
					items: M(b.value.item_group || b.value.key),
					ui: d.value,
					options: {
						...N(b.value),
						selector: b.value.selector !== !1
					},
					run: A,
					busy: O
				}, null, 8, [
					"items",
					"ui",
					"options"
				])) : Z("", !0)])) : (q(), da(bm, {
					key: 2,
					items: M(v.value?.item_group),
					ui: d.value,
					options: N(v.value),
					run: A,
					busy: O
				}, null, 8, [
					"items",
					"ui",
					"options"
				]))], 64)) : (q(), J(K, { key: 2 }, [h.value.action ? (q(), J("form", {
					key: 0,
					class: "form-grid core-manager-add-form tcx-native-add-form",
					onSubmit: bs(F, ["prevent"])
				}, [(q(!0), J(K, null, G(g.value, (e) => (q(), da(mp, {
					key: e.key,
					field: e,
					"model-value": s[e.key],
					"all-values": s,
					"onUpdate:modelValue": (t) => P(e, t),
					onError: o[3] ||= (e) => a.value = e
				}, null, 8, [
					"field",
					"model-value",
					"all-values",
					"onUpdate:modelValue"
				]))), 128)), Y("div", Gm, [Y("button", {
					class: "action-btn",
					type: "submit",
					disabled: O(`add:${h.value.action}`)
				}, V(h.value.submit_label || "Add"), 9, Km)])], 32)) : Z("", !0), ga(bm, {
					items: M(""),
					ui: d.value,
					options: { empty_message: u.value.empty_message },
					run: A,
					busy: O
				}, null, 8, [
					"items",
					"ui",
					"options"
				])], 64))
			])
		], 10, Dm)) : (q(), J("div", qm, [
			Y("div", Jm, [Y("h3", Ym, V(e.tab.label || e.tab.core_key), 1), Y("span", Xm, V(e.tab.core_key), 1)]),
			u.value.summary ? (q(), J("div", Zm, V(u.value.summary), 1)) : Z("", !0),
			p.value.length ? (q(), J("div", Qm, [(q(!0), J(K, null, G(p.value, (e) => (q(), J("div", {
				key: e.label,
				class: "core-metric-pill"
			}, [Y("div", $m, V(e.label), 1), Y("div", null, V(e.value ?? "-"), 1)]))), 128))])) : Z("", !0),
			u.value.items?.length ? (q(), J("div", eh, [(q(!0), J(K, null, G(u.value.items, (e, t) => (q(), J("article", {
				key: e.id || e.title || t,
				class: "core-tab-item"
			}, [
				Y("div", th, [Y("h3", nh, V(e.title || "(untitled)"), 1)]),
				e.subtitle ? (q(), J("div", rh, V(e.subtitle), 1)) : Z("", !0),
				e.detail ? (q(), J("div", ih, V(e.detail), 1)) : Z("", !0)
			]))), 128))])) : (q(), J("div", ah, V(u.value.empty_message || "No data available for this tab."), 1))
		]))]));
	}
}), sh = { class: "tater-vue-surface tcx-cores" }, ch = { class: "tv-page-heading" }, lh = { class: "tv-heading-actions" }, uh = { class: "tv-metrics" }, dh = {
	key: 1,
	class: "tv-notice error"
}, fh = {
	class: "tv-tabs tcx-top-tabs core-top-tabs",
	"aria-label": "Core panels"
}, ph = ["data-core-tab", "onClick"], mh = {
	key: 0,
	class: "tcx-tab-dot",
	title: "Core is stopped"
}, hh = { key: 0 }, gh = ["data-core-tab-panel", "data-core-tab-loaded"], _h = {
	key: 0,
	class: "tv-empty"
}, vh = {
	key: 3,
	class: "core-top-tab-panel active tcx-manage",
	"data-core-tab-panel": "manage"
}, yh = {
	class: "tv-mini-tabs tcx-manage-tabs",
	"aria-label": "Core management"
}, bh = ["onClick"], xh = { key: 0 }, Sh = {
	key: 0,
	class: "tcx-card-grid"
}, Ch = { class: "tv-eyebrow" }, wh = { class: "tp-version" }, Th = ["onClick"], Eh = { key: 1 }, Dh = ["onClick"], Oh = {
	key: 0,
	class: "tv-empty"
}, kh = {
	key: 1,
	class: "tcx-card-grid"
}, Ah = { class: "tv-eyebrow" }, jh = { class: "tv-state" }, Mh = ["onClick"], Nh = {
	key: 0,
	class: "tv-empty"
}, Ph = {
	key: 2,
	class: "tcx-manage-list tv-manage-workspace"
}, Fh = { class: "tv-panel tcx-manage-toolbar tv-manage-hero" }, Ih = { class: "tv-manage-overview" }, Lh = ["disabled"], Rh = { class: "tv-manage-identity" }, zh = { class: "tv-manage-monogram" }, Bh = { class: "tv-eyebrow" }, Vh = { class: "tv-manage-version" }, Hh = { class: "tv-manage-actions" }, Uh = ["disabled", "onClick"], Wh = ["onClick"], Gh = { class: "ti-purge" }, Kh = ["onUpdate:modelValue"], qh = ["onClick"], Jh = {
	key: 0,
	class: "tv-empty"
}, Yh = {
	key: 3,
	class: "tv-panel tcx-repos tv-repository-manager"
}, Xh = {
	class: "tv-repository-tabs",
	"aria-label": "Core repository sources"
}, Zh = {
	key: 0,
	class: "tv-trusted-repositories"
}, Qh = {
	key: 0,
	class: "tv-repository-warning"
}, $h = { class: "tv-trusted-repo-grid" }, eg = {
	class: "tv-trusted-repo-card builtin selected",
	"aria-label": "Built-in Tater Core Shop repository"
}, tg = { class: "tv-repo-card-top" }, ng = [
	"aria-pressed",
	"onClick",
	"onKeydown"
], rg = { class: "tv-repo-check" }, ig = { class: "tv-repo-card-top" }, ag = { class: "tv-repo-monogram" }, og = ["href"], sg = { key: 1 }, cg = {
	key: 0,
	class: "ti-tags"
}, lg = ["href"], ug = {
	key: 1,
	class: "tv-empty compact"
}, dg = {
	key: 1,
	class: "tv-custom-repositories"
}, fg = ["onClick"], pg = {
	key: 0,
	class: "tv-empty compact"
}, mg = { class: "tcx-repo-form" }, hg = {
	class: "tv-modal tcx-settings-modal",
	role: "dialog",
	"aria-modal": "true"
}, gg = { class: "tvb-field-grid" }, _g = /* @__PURE__ */ sr({
	__name: "CoresApp",
	props: {
		state: {},
		options: {}
	},
	setup(e, { expose: t }) {
		let n = e, r = [
			{
				id: "installed",
				label: "Installed"
			},
			{
				id: "store",
				label: "Store"
			},
			{
				id: "manage",
				label: "Manage"
			},
			{
				id: "repos",
				label: "Repositories"
			}
		], i = /* @__PURE__ */ U(String(n.options.initialTab || "manage")), a = /* @__PURE__ */ U("installed"), o = /* @__PURE__ */ U(""), s = /* @__PURE__ */ U(""), c = /* @__PURE__ */ U(""), l = /* @__PURE__ */ U({}), u = /* @__PURE__ */ U(""), d = /* @__PURE__ */ U(""), f = /* @__PURE__ */ U([]), p = /* @__PURE__ */ U("trusted"), m = /* @__PURE__ */ U(null), h = /* @__PURE__ */ U({}), g = /* @__PURE__ */ Et({}), _ = /* @__PURE__ */ U({}), v = null, y = 0, b = Q(() => n.state.payload?.runtime || {}), x = Q(() => n.state.payload?.shop || {}), S = Q(() => n.state.payload?.tabs || {}), C = Q(() => Array.isArray(b.value.items) ? b.value.items : []), w = Q(() => Array.isArray(x.value.installed) ? x.value.installed : []), T = Q(() => Array.isArray(x.value.catalog) ? x.value.catalog : []), E = Q(() => T.value.filter((e) => !e.installed).sort(ie)), D = Q(() => w.value.filter((e) => e.update_available)), O = Q(() => C.value.filter((e) => !!e.running).length), k = Q(() => Array.isArray(x.value.repos?.trusted) ? x.value.repos.trusted : []), A = Q(() => (Array.isArray(S.value.tabs) ? S.value.tabs : []).filter((e) => L(e.core_key)).map((e) => ({
			...e,
			core_key: L(e.core_key)
		}))), j = Q(() => /* @__PURE__ */ new Set(["manage", ...A.value.map((e) => e.core_key)])), M = Q(() => A.value.find((e) => e.core_key === i.value) || null), N = Q(() => g[i.value] || null), P = Q(() => N.value?.payload || {}), F = Q(() => L(P.value?.ui?.appearance).toLowerCase() === "music_library"), I = Q(() => new Map(C.value.map((e) => [R(e.key), e]))), ee = Q(() => {
			let e = /* @__PURE__ */ new Map();
			return w.value.forEach((t) => {
				let n = L(t.module_key || `${t.id}_core`);
				n && e.set(R(n), t), t.id && e.set(R(t.id), t);
			}), e;
		}), te = Q(() => {
			let e = /* @__PURE__ */ new Set(), t = C.value.map((t) => {
				let n = L(t.key), r = ee.value.get(R(n)) || ee.value.get(R(re(n))) || null;
				return r && e.add(R(r.id)), {
					key: n,
					runtime: t,
					shop: r
				};
			});
			return w.value.forEach((n) => {
				e.has(R(n.id)) || t.push({
					key: L(n.module_key || `${n.id}_core`),
					runtime: null,
					shop: n
				});
			}), t.sort((e, t) => ae(e).localeCompare(ae(t), void 0, {
				sensitivity: "base",
				numeric: !0
			}));
		}), ne = Q(() => {
			let e = M.value?.core_key;
			if (!e) return null;
			let t = encodeURIComponent(e);
			return {
				initialPayload: P.value,
				coreKey: e,
				tabEndpoint: `${n.options.endpoints.runtime}/${t}/tab`,
				actionEndpoint: `${n.options.endpoints.runtime}/${t}/tab-action`,
				eventsEndpoint: `${n.options.endpoints.runtime}/${t}/tab-events`,
				onToast: n.options.onToast
			};
		});
		function L(e) {
			return String(e ?? "").trim();
		}
		function R(e) {
			return L(e).toLowerCase();
		}
		function z(e) {
			return encodeURIComponent(L(e));
		}
		function re(e) {
			return L(e).replace(/_core$/i, "");
		}
		function ie(e, t) {
			return L(e.name || e.id).localeCompare(L(t.name || t.id), void 0, {
				sensitivity: "base",
				numeric: !0
			});
		}
		function ae(e) {
			return L(e.runtime?.label || e.shop?.name || re(e.key));
		}
		function oe(e) {
			return L(e.shop?.description || "Local Core module.");
		}
		function se(e) {
			let t = L(e.module_key || `${e.id}_core`);
			return I.value.get(R(t)) || I.value.get(R(e.id)) || null;
		}
		function ce(e) {
			return e ? e.running ? "Running" : e.desired_running ? "Pending start" : "Stopped" : "Unavailable";
		}
		function le(e, t = "success") {
			s.value = e, c.value = t === "error" ? e : "", n.options.onToast?.(e, t);
		}
		function ue() {
			let e = new Set(k.value.map((e) => L(e.url).toLowerCase()).filter(Boolean));
			f.value = Array.isArray(x.value.repos?.additional) ? x.value.repos.additional.filter((t) => !e.has(L(t.url).toLowerCase())).map((e) => ({ ...e })) : [];
		}
		function de() {
			return k.value.filter((e) => !!e.enabled).map((e) => ({
				name: L(e.name),
				url: L(e.url)
			}));
		}
		function fe() {
			return [...de(), ...f.value];
		}
		async function pe(e) {
			if (o.value) return;
			let t = !!e.enabled, r = !e.enabled;
			e.enabled = r, o.value = `${r ? "Adding" : "Removing"} ${L(e.name || e.repository)}…`;
			try {
				await js(`${n.options.endpoints.shop}/repos`, { repos: fe() }), le(`${L(e.name || e.repository)} ${r ? "added to" : "removed from"} the Core Store.`), await he(!0);
			} catch (n) {
				e.enabled = t, le(n instanceof Error ? n.message : "Trusted repository update failed.", "error");
			} finally {
				o.value = "";
			}
		}
		function me(e) {
			return g[e] || (g[e] = { payload: {} }), g[e];
		}
		async function he(e = !1) {
			e || (o.value = "Refreshing Cores…"), c.value = "";
			try {
				let [e, t, r] = await Promise.all([
					As(n.options.endpoints.runtime),
					As(n.options.endpoints.shop),
					As(n.options.endpoints.tabs)
				]);
				n.state.payload = {
					runtime: e,
					shop: t,
					tabs: r
				}, ue(), j.value.has(i.value) ? i.value !== "manage" && await ge(i.value, !0) : await be("manage");
			} catch (e) {
				le(e instanceof Error ? e.message : "Core refresh failed.", "error");
			} finally {
				e || (o.value = "");
			}
		}
		async function ge(e, t = !1) {
			let r = L(e);
			if (!r || r === "manage") {
				await he(t);
				return;
			}
			_.value = {
				..._.value,
				[r]: !0
			};
			try {
				let e = await As(`${n.options.endpoints.runtime}/${z(r)}/tab`);
				me(r).payload = e || {}, r === i.value && !_e(e) && ye(r, e);
			} catch (e) {
				me(r).payload = { error: e instanceof Error ? e.message : "Core panel failed to load." };
			} finally {
				_.value = {
					..._.value,
					[r]: !1
				};
			}
		}
		function _e(e) {
			return L(e?.ui?.appearance).toLowerCase() === "music_library";
		}
		function ve() {
			v?.close(), v = null, y && window.clearTimeout(y), y = 0;
		}
		function ye(e, t) {
			ve(), !(i.value !== e || _e(t) || !t?.ui?.live_updates) && (v = new EventSource(`${n.options.endpoints.runtime}/${z(e)}/tab-events`), v.addEventListener("core-tab", (t) => {
				try {
					me(e).payload = JSON.parse(t.data);
				} catch {}
			}), v.addEventListener("error", () => {
				v?.close(), v = null, i.value === e && (y = window.setTimeout(() => ye(e, me(e).payload), 3e3));
			}));
		}
		async function be(e) {
			let t = j.value.has(e) ? e : "manage";
			if (i.value = t, n.options.onTabChange?.(t), ve(), t !== "manage") {
				let e = me(t);
				Object.keys(e.payload).length ? _e(e.payload) || ye(t, e.payload) : await ge(t);
			}
		}
		async function xe(e, t) {
			let r = L(e.key);
			if (r) {
				o.value = `${t === "start" ? "Starting" : "Stopping"} ${r}…`;
				try {
					await js(`${n.options.endpoints.runtime}/${z(r)}/${t}`), le(`${L(e.label || r)} ${t === "start" ? "started" : "stopped"}.`), await he(!0), n.options.onHealthRefresh?.();
				} catch (e) {
					le(e instanceof Error ? e.message : `Core ${t} failed.`, "error");
				} finally {
					o.value = "";
				}
			}
		}
		async function H(e, t = "") {
			if (!(e === "remove" && !window.confirm(`Remove ${t}?${l.value[t] ? " Its saved data will also be deleted." : ""}`))) {
				o.value = `${e.replaceAll("-", " ")} ${t || "Cores"}…`;
				try {
					let r = t ? { id: t } : {};
					e === "remove" && (r.purge_redis = !!l.value[t]);
					let i = await js(`${n.options.endpoints.shop}/${e}`, r), o = Array.isArray(i.updated) ? i.updated.length : 0, s = Array.isArray(i.failed) ? i.failed.length : 0;
					le(L(i.message) || (e === "update-all" ? `Update-all completed. Updated ${o}, failed ${s}.` : "Core action completed."), s ? "error" : "success"), await he(!0), e === "install" && (a.value = "installed"), n.options.onHealthRefresh?.();
				} catch (e) {
					le(e instanceof Error ? e.message : "Core action failed.", "error");
				} finally {
					o.value = "";
				}
			}
		}
		function Se(e) {
			let t = e.value ?? e.default ?? "", n = L(e.type).toLowerCase();
			return n === "checkbox" ? typeof t == "string" ? [
				"1",
				"true",
				"yes",
				"on",
				"enabled"
			].includes(t.toLowerCase()) : !!t : n === "number" || n === "range" ? t === "" ? "" : Number(t) : n === "multiselect" ? Array.isArray(t) ? [...t] : L(t).split(",").map((e) => e.trim()).filter(Boolean) : t;
		}
		function Ce(e) {
			return (Array.isArray(e.show_when_all) ? e.show_when_all : e.show_when && typeof e.show_when == "object" ? [e.show_when] : []).every((e) => {
				let t = L(e.source_key ?? e.key);
				if (!t) return !0;
				let n = [
					...e.any_of || [],
					...e.values || [],
					...e.equals === void 0 ? [] : [e.equals],
					...e.value === void 0 ? [] : [e.value]
				].map((e) => String(e ?? "").trim()), r = typeof h.value[t] == "boolean" ? h.value[t] ? "true" : "false" : String(h.value[t] ?? "").trim();
				return !n.length || n.includes(r);
			});
		}
		function we(e) {
			m.value = e, h.value = Object.fromEntries((Array.isArray(e.settings) ? e.settings : []).filter((e) => L(e.key)).map((e) => [L(e.key), Se(e)]));
		}
		async function Te() {
			let e = m.value;
			if (!e) return;
			let t = L(e.key);
			o.value = `Saving ${L(e.label || t)}…`;
			try {
				let r = Object.fromEntries((e.settings || []).filter((e) => L(e.key) && ![
					"section",
					"header",
					"readonly",
					"read_only",
					"led_preview"
				].includes(L(e.type).toLowerCase()) && Ce(e)).map((e) => [L(e.key), h.value[L(e.key)]]));
				await js(`${n.options.endpoints.runtime}/${z(t)}/settings`, { values: r }), le(`Saved settings for ${L(e.label || t)}.`), m.value = null, await he(!0);
			} catch (e) {
				le(e instanceof Error ? e.message : "Core settings save failed.", "error");
			} finally {
				o.value = "";
			}
		}
		function Ee() {
			let e = d.value.trim();
			if (!e) {
				le("Repository URL is required.", "error");
				return;
			}
			if ([...f.value, ...k.value].some((t) => L(t.url).toLowerCase() === e.toLowerCase())) {
				le("That repository is already listed.", "error");
				return;
			}
			f.value.push({
				name: u.value.trim(),
				url: e
			}), u.value = "", d.value = "", s.value = "Repository added. Save repositories to apply it.", c.value = "";
		}
		async function De() {
			o.value = "Saving Core repositories…";
			try {
				await js(`${n.options.endpoints.shop}/repos`, { repos: fe() }), le("Core repositories saved."), await he(!0);
			} catch (e) {
				le(e instanceof Error ? e.message : "Repository save failed.", "error");
			} finally {
				o.value = "";
			}
		}
		function Oe(e) {
			e.key === "Escape" && (m.value = null);
		}
		return On(() => n.state.payload, ue, { deep: !1 }), On(A, (e) => {
			(/* @__PURE__ */ new Set(["manage", ...e.map((e) => e.core_key)])).has(i.value) || be("manage");
		}, { immediate: !0 }), ue(), window.addEventListener("keydown", Oe), kr(() => {
			ve(), window.removeEventListener("keydown", Oe);
		}), un(() => void be(i.value)), t({
			refresh: () => he(!1),
			refreshTab: (e) => ge(e, !0),
			select: be
		}), (t, n) => (q(), J(K, null, [Y("div", sh, [
			Y("header", ch, [n[12] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "System capabilities"),
				Y("h1", null, "Cores"),
				Y("p", null, "Run, configure, browse, and update Tater’s capability modules from one live workspace.")
			], -1), Y("div", lh, [Y("span", { class: B(["tv-live-pill", { busy: !!o.value }]) }, [n[11] ||= Y("i", null, null, -1), X(V(o.value || "Live"), 1)], 2), Y("button", {
				class: "tv-button",
				type: "button",
				onClick: n[0] ||= (e) => he()
			}, "Refresh")])]),
			Y("div", uh, [
				Y("div", null, [n[13] ||= Y("span", null, "Installed", -1), Y("strong", null, V(w.value.length || C.value.length), 1)]),
				Y("div", null, [n[14] ||= Y("span", null, "Running", -1), Y("strong", null, V(O.value), 1)]),
				Y("div", null, [n[15] ||= Y("span", null, "Panels", -1), Y("strong", null, V(A.value.length), 1)]),
				Y("div", null, [n[16] ||= Y("span", null, "Updates", -1), Y("strong", null, V(Number(x.value.updates_available || D.value.length)), 1)])
			]),
			s.value || c.value ? (q(), J("div", {
				key: 0,
				class: B(["tv-notice", { error: !!c.value }])
			}, V(c.value || s.value), 3)) : Z("", !0),
			x.value.errors?.length ? (q(), J("div", dh, V(x.value.errors.join(" • ")), 1)) : Z("", !0),
			Y("nav", fh, [(q(!0), J(K, null, G(A.value, (e) => (q(), J("button", {
				key: e.core_key,
				type: "button",
				class: B(["core-top-tab-btn", { active: i.value === e.core_key }]),
				"data-core-tab": e.core_key,
				onClick: (t) => be(e.core_key)
			}, [X(V(e.label || e.core_key), 1), e.requires_running && !e.running ? (q(), J("span", mh)) : Z("", !0)], 10, ph))), 128)), Y("button", {
				type: "button",
				class: B(["core-top-tab-btn", { active: i.value === "manage" }]),
				"data-core-tab": "manage",
				onClick: n[1] ||= (e) => be("manage")
			}, [X(V(S.value.manage_label || "Manage"), 1), D.value.length ? (q(), J("span", hh, V(D.value.length), 1)) : Z("", !0)], 2)]),
			i.value === "manage" ? (q(), J("section", vh, [Y("nav", yh, [(q(), J(K, null, G(r, (e) => Y("button", {
				key: e.id,
				type: "button",
				class: B({ active: a.value === e.id }),
				onClick: (t) => a.value = e.id
			}, [X(V(e.label), 1), e.id === "manage" && D.value.length ? (q(), J("span", xh, V(D.value.length), 1)) : Z("", !0)], 10, bh)), 64))]), a.value === "installed" ? (q(), J("div", Sh, [(q(!0), J(K, null, G(te.value, (e) => (q(), J("article", {
				key: e.key,
				class: "tv-panel tcx-core-card"
			}, [
				Y("header", null, [Y("div", null, [Y("span", Ch, V(e.key), 1), Y("h2", null, V(ae(e)), 1)]), Y("span", { class: B(["tv-state", {
					good: e.runtime?.running,
					pending: e.runtime?.desired_running && !e.runtime?.running
				}]) }, V(ce(e.runtime)), 3)]),
				Y("p", null, V(oe(e)), 1),
				Y("div", wh, [
					Y("span", null, "Installed " + V(e.shop?.installed_ver || "0.0.0"), 1),
					Y("span", null, "Store " + V(e.shop?.store_ver || "-"), 1),
					Y("span", null, V(e.shop?.source_label || "local"), 1)
				]),
				Y("footer", null, [e.runtime?.settings?.length ? (q(), J("button", {
					key: 0,
					class: "tv-button",
					type: "button",
					onClick: (t) => we(e.runtime)
				}, "Settings", 8, Th)) : (q(), J("span", Eh, V(e.runtime ? "No configurable settings" : "Runtime unavailable"), 1)), e.runtime ? (q(), J("button", {
					key: 2,
					class: B(["tv-button", { primary: !e.runtime.running }]),
					type: "button",
					onClick: (t) => xe(e.runtime, e.runtime.running ? "stop" : "start")
				}, V(e.runtime.running ? "Stop" : "Start"), 11, Dh)) : Z("", !0)])
			]))), 128)), te.value.length ? Z("", !0) : (q(), J("div", Oh, "No installed Cores found."))])) : a.value === "store" ? (q(), J("div", kh, [(q(!0), J(K, null, G(E.value, (e) => (q(), J("article", {
				key: e.id,
				class: "tv-panel tcx-core-card"
			}, [
				Y("header", null, [Y("div", null, [Y("span", Ah, V(e.id), 1), Y("h2", null, V(e.name || e.id), 1)]), Y("span", jh, "v" + V(e.version || "-"), 1)]),
				Y("p", null, V(e.description || "No description provided."), 1),
				Y("footer", null, [Y("span", null, V(e.source_label || "Tater Shop"), 1), Y("button", {
					class: "tv-button primary",
					type: "button",
					onClick: (t) => H("install", e.id)
				}, "Install", 8, Mh)])
			]))), 128)), E.value.length ? Z("", !0) : (q(), J("div", Nh, "No additional Cores are available from the configured repositories."))])) : a.value === "manage" ? (q(), J("div", Ph, [
				Y("div", Fh, [n[19] ||= Y("div", { class: "tv-manage-hero-copy" }, [
					Y("span", { class: "tv-eyebrow" }, "Manage library"),
					Y("h2", null, "Core control center"),
					Y("p", null, "Update system capabilities, control their runtimes, and remove Cores with optional data cleanup. Running Cores restart automatically after an update.")
				], -1), Y("div", Ih, [
					Y("div", null, [n[17] ||= Y("span", null, "Installed", -1), Y("strong", null, V(w.value.length), 1)]),
					Y("div", { class: B({ attention: D.value.length }) }, [n[18] ||= Y("span", null, "Updates ready", -1), Y("strong", null, V(D.value.length), 1)], 2),
					Y("button", {
						class: "tv-button primary",
						type: "button",
						disabled: !D.value.length,
						onClick: n[2] ||= (e) => H("update-all")
					}, "Update all", 8, Lh)
				])]),
				(q(!0), J(K, null, G(w.value.slice().sort(ie), (e) => (q(), J("article", {
					key: e.id,
					class: B(["tv-panel tcx-manage-row tv-manage-card", { "has-update": e.update_available }])
				}, [
					Y("div", Rh, [Y("span", zh, V(L(e.name || e.id).charAt(0).toUpperCase()), 1), Y("div", null, [
						Y("span", Bh, V(e.id), 1),
						Y("h3", null, V(e.name || e.id), 1),
						Y("small", null, V(e.source_label || "Local Core"), 1)
					])]),
					Y("div", Vh, [
						Y("div", null, [n[20] ||= Y("span", null, "Installed", -1), Y("strong", null, V(e.installed_ver || "0.0.0"), 1)]),
						n[22] ||= Y("i", null, "→", -1),
						Y("div", null, [n[21] ||= Y("span", null, "Latest", -1), Y("strong", null, V(e.store_ver || "-"), 1)]),
						Y("span", { class: B(["tv-state", e.update_available ? "pending" : "good"]) }, V(e.update_available ? "Update ready" : "Current"), 3)
					]),
					Y("div", Hh, [
						Y("span", { class: B(["tv-manage-runtime", { online: se(e)?.running }]) }, [n[23] ||= Y("i", null, null, -1), X(V(ce(se(e))), 1)], 2),
						Y("button", {
							class: B(["tv-button", { primary: e.update_available }]),
							type: "button",
							disabled: !e.update_available,
							onClick: (t) => H("update", e.id)
						}, V(e.update_available ? "Update" : "Current"), 11, Uh),
						se(e) ? (q(), J("button", {
							key: 0,
							class: "tv-button",
							type: "button",
							onClick: (t) => xe(se(e), se(e)?.running ? "stop" : "start")
						}, V(se(e)?.running ? "Stop" : "Start"), 9, Wh)) : Z("", !0),
						Y("label", Gh, [W(Y("input", {
							"onUpdate:modelValue": (t) => l.value[e.id] = t,
							type: "checkbox"
						}, null, 8, Kh), [[cs, l.value[e.id]]]), n[24] ||= X(" Delete data", -1)]),
						Y("button", {
							class: "tv-button danger",
							type: "button",
							onClick: (t) => H("remove", e.id)
						}, "Remove", 8, qh)
					])
				], 2))), 128)),
				w.value.length ? Z("", !0) : (q(), J("div", Jh, "No installed Cores found."))
			])) : (q(), J("div", Yh, [
				n[37] ||= Y("header", { class: "tv-repository-heading" }, [Y("div", null, [
					Y("span", { class: "tv-eyebrow" }, "Repository library"),
					Y("h2", null, "Core repositories"),
					Y("p", null, "Choose a Tater-trusted source or add your own manifest.")
				])], -1),
				Y("nav", Xh, [Y("button", {
					type: "button",
					class: B({ active: p.value === "trusted" }),
					onClick: n[3] ||= (e) => p.value = "trusted"
				}, "Trusted repositories", 2), Y("button", {
					type: "button",
					class: B({ active: p.value === "custom" }),
					onClick: n[4] ||= (e) => p.value = "custom"
				}, "Custom repositories", 2)]),
				p.value === "trusted" ? (q(), J("div", Zh, [
					n[33] ||= Y("p", { class: "tv-repository-intro" }, "Curated sources are reviewed by Tater. Select a card to add or remove its Cores from your Store automatically.", -1),
					x.value.repos?.trusted_error ? (q(), J("div", Qh, "The trusted directory is temporarily unavailable. Your enabled repositories are unchanged.")) : Z("", !0),
					Y("div", $h, [Y("article", eg, [
						n[28] ||= Y("span", { class: "tv-repo-check" }, "✓", -1),
						Y("div", tg, [n[27] ||= Y("span", { class: "tv-repo-monogram" }, "T", -1), Y("div", null, [
							n[25] ||= Y("span", { class: "tv-eyebrow" }, "Always available", -1),
							Y("h3", null, V(x.value.repos?.default?.name || "Tater Core Shop"), 1),
							n[26] ||= Y("p", null, [X("by "), Y("strong", null, "Tater Assistant")], -1)
						])]),
						n[29] ||= Y("p", null, "The official built-in Core catalog.", -1),
						n[30] ||= Y("footer", null, [Y("span", { class: "tv-repo-enabled" }, "Built in")], -1)
					]), (q(!0), J(K, null, G(k.value, (e) => (q(), J("article", {
						key: e.id || e.url,
						class: B(["tv-trusted-repo-card", { selected: e.enabled }]),
						role: "button",
						tabindex: "0",
						"aria-pressed": !!e.enabled,
						onClick: (t) => pe(e),
						onKeydown: [Ss(bs((t) => pe(e), ["prevent"]), ["enter"]), Ss(bs((t) => pe(e), ["prevent"]), ["space"])]
					}, [
						Y("span", rg, V(e.enabled ? "✓" : "+"), 1),
						Y("div", ig, [Y("span", ag, V(L(e.repository || e.name).charAt(0).toUpperCase()), 1), Y("div", null, [
							n[32] ||= Y("span", { class: "tv-eyebrow" }, "Trusted Core source", -1),
							Y("h3", null, V(e.repository || e.name), 1),
							Y("p", null, [n[31] ||= X("by ", -1), e.author_url ? (q(), J("a", {
								key: 0,
								href: e.author_url,
								target: "_blank",
								rel: "noreferrer",
								onClick: n[5] ||= bs(() => {}, ["stop"])
							}, V(e.author || "Community author"), 9, og)) : (q(), J("strong", sg, V(e.author || "Community author"), 1))])
						])]),
						Y("p", null, V(e.description || "Additional Cores for the Tater Store."), 1),
						e.tags?.length ? (q(), J("div", cg, [(q(!0), J(K, null, G(e.tags, (e) => (q(), J("span", { key: e }, V(e), 1))), 128))])) : Z("", !0),
						Y("footer", null, [e.homepage ? (q(), J("a", {
							key: 0,
							href: e.homepage,
							target: "_blank",
							rel: "noreferrer",
							onClick: n[6] ||= bs(() => {}, ["stop"])
						}, "View repository ↗", 8, lg)) : Z("", !0), Y("span", { class: B(e.enabled ? "tv-repo-enabled" : "tv-repo-available") }, V(e.enabled ? "Added to Store" : "Select to add"), 3)])
					], 42, ng))), 128))]),
					k.value.length ? Z("", !0) : (q(), J("div", ug, "No additional trusted Core repositories are listed yet."))
				])) : (q(), J("div", dg, [
					n[36] ||= Y("p", { class: "tv-repository-intro" }, "Custom manifests are managed by you and are not reviewed by Tater.", -1),
					(q(!0), J(K, null, G(f.value, (e, t) => (q(), J("article", {
						key: `${e.url}-${t}`,
						class: "ti-repo-row"
					}, [Y("div", null, [Y("strong", null, V(e.name || "Custom repository"), 1), Y("code", null, V(e.url), 1)]), Y("button", {
						class: "tv-button",
						type: "button",
						onClick: (e) => f.value.splice(t, 1)
					}, "Remove", 8, fg)]))), 128)),
					f.value.length ? Z("", !0) : (q(), J("div", pg, "No custom repositories configured.")),
					Y("div", mg, [
						Y("label", null, [n[34] ||= Y("span", null, "Name (optional)", -1), W(Y("input", {
							"onUpdate:modelValue": n[7] ||= (e) => u.value = e,
							type: "text",
							placeholder: "My Core Repo"
						}, null, 512), [[$, u.value]])]),
						Y("label", null, [n[35] ||= Y("span", null, "Manifest URL", -1), W(Y("input", {
							"onUpdate:modelValue": n[8] ||= (e) => d.value = e,
							type: "url",
							placeholder: "https://example.com/cores.json",
							onKeyup: Ss(Ee, ["enter"])
						}, null, 544), [[$, d.value]])]),
						Y("button", {
							class: "tv-button",
							type: "button",
							onClick: Ee
						}, "Add"),
						Y("button", {
							class: "tv-button primary",
							type: "button",
							onClick: De
						}, "Save custom repositories")
					])
				]))
			]))])) : (q(), J("section", {
				key: 2,
				class: "core-top-tab-panel active tcx-core-panel",
				"data-core-tab-panel": i.value,
				"data-core-tab-loaded": _.value[i.value] ? "loading" : "1"
			}, [_.value[i.value] && !Object.keys(P.value).length ? (q(), J("div", _h, "Loading " + V(M.value?.label || i.value) + "…", 1)) : M.value && N.value && F.value && ne.value ? (q(), da(dd, {
				key: 1,
				state: N.value,
				options: ne.value
			}, null, 8, ["state", "options"])) : M.value && N.value ? (q(), da(oh, {
				key: 2,
				payload: P.value,
				tab: M.value,
				"action-endpoint": `${e.options.endpoints.runtime}/${z(M.value.core_key)}/tab-action`,
				refresh: () => ge(M.value.core_key, !0),
				notify: le
			}, null, 8, [
				"payload",
				"tab",
				"action-endpoint",
				"refresh"
			])) : Z("", !0)], 8, gh))
		]), ga(kl, {
			open: !!m.value,
			onClose: n[10] ||= (e) => m.value = null
		}, {
			default: Sn(() => [Y("section", hg, [
				Y("header", null, [Y("div", null, [
					n[38] ||= Y("span", { class: "tv-eyebrow" }, "Core settings", -1),
					Y("h2", null, V(m.value?.label || m.value?.key), 1),
					n[39] ||= Y("p", null, "Changes are applied to this Core’s runtime configuration.", -1)
				]), Y("button", {
					class: "tv-button",
					type: "button",
					onClick: n[9] ||= (e) => m.value = null
				}, "Close")]),
				Y("div", gg, [(q(!0), J(K, null, G(m.value?.settings || [], (e) => (q(), da(zd, {
					key: e.key || e.label,
					field: e,
					"model-value": h.value[e.key],
					"all-values": h.value,
					"onUpdate:modelValue": (t) => h.value[e.key] = t
				}, null, 8, [
					"field",
					"model-value",
					"all-values",
					"onUpdate:modelValue"
				]))), 128))]),
				Y("footer", null, [Y("span", null, V(o.value || "Ready"), 1), Y("button", {
					class: "tv-button primary",
					type: "button",
					onClick: Te
				}, "Save settings")])
			])]),
			_: 1
		}, 8, ["open"])], 64));
	}
}), vg = { class: "tater-vue-surface td-dashboard" }, yg = { class: "tv-page-heading" }, bg = { key: 0 }, xg = { key: 1 }, Sg = { class: "tv-heading-actions" }, Cg = {
	key: 0,
	class: "tv-notice error"
}, wg = {
	key: 1,
	class: "td-overview tv-panel"
}, Tg = {
	key: 2,
	class: "tv-panel td-updates"
}, Eg = { class: "tv-panel-head" }, Dg = { class: "td-update-grid" }, Og = ["onClick"], kg = { key: 0 }, Ag = { key: 1 }, jg = {
	key: 3,
	class: "tv-panel td-system-brief"
}, Mg = { class: "tv-panel-head" }, Ng = { class: "tv-eyebrow" }, Pg = { key: 0 }, Fg = {
	key: 0,
	class: "td-brief"
}, Ig = {
	key: 1,
	class: "tv-metrics"
}, Lg = {
	key: 2,
	class: "td-items"
}, Rg = ["src", "alt"], zg = {
	class: "tv-modal td-dashboard-controls",
	role: "dialog",
	"aria-modal": "true",
	"aria-label": "Dashboard controls"
}, Bg = { class: "tv-control-list" }, Vg = { class: "tv-toggle" }, Hg = { class: "tv-toggle" }, Ug = ["value"], Wg = ["value"], Gg = ["value"], Kg = /* @__PURE__ */ sr({
	__name: "DashboardApp",
	props: {
		state: {},
		options: {}
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ U(!1), r = /* @__PURE__ */ U(""), i = /* @__PURE__ */ U(""), a = /* @__PURE__ */ U(t.options.initialPreferences?.showMetrics !== !1), o = /* @__PURE__ */ U(t.options.initialPreferences?.showMedia !== !1), s = 0, c = 0, l = null, u = /* @__PURE__ */ U(null), d = Q(() => t.state.payload || {}), f = Q(() => Array.isArray(d.value.sections) ? d.value.sections : []), p = Q(() => [...f.value].sort((e, t) => T(e?.title || e?.id).localeCompare(T(t?.title || t?.id), void 0, { sensitivity: "base" }))), m = Q(() => Array.isArray(d.value.briefs) ? d.value.briefs : []), h = Q(() => new Map(m.value.map((e) => [String(e?.id || ""), e]))), g = Q(() => d.value.updates && typeof d.value.updates == "object" ? d.value.updates : {}), _ = Q(() => Array.isArray(g.value.groups) ? g.value.groups : []), v = Q(() => d.value.settings?.personal || {}), y = Q(() => d.value.settings?.refresh || {}), b = /* @__PURE__ */ U(""), x = /* @__PURE__ */ U(300), S = /* @__PURE__ */ U(3600), C = Q(() => h.value.get("overview")), w = Q(() => h.value.get("system"));
		function T(e) {
			return String(e ?? "").trim();
		}
		function E(e) {
			let t = Number(e || 0);
			return !Number.isFinite(t) || t <= 0 ? "" : new Intl.DateTimeFormat(void 0, {
				hour: "numeric",
				minute: "2-digit"
			}).format(/* @__PURE__ */ new Date(t * 1e3));
		}
		function D(e) {
			return h.value.get(T(e?.id));
		}
		function O(e) {
			for (let t of [
				"outlook_items",
				"snapshots",
				"visuals",
				"recent_events",
				"events",
				"devices"
			]) {
				let n = e?.[t];
				if (Array.isArray(n) && n.length) return n.slice(0, 8);
			}
			return [];
		}
		function k(e) {
			return T(e?.image_src || e?.hero_image_src || e?.image);
		}
		function A(e) {
			let n = T(e);
			n && t.options.onNavigate?.(n);
		}
		function j(e, n = "success") {
			t.options.onToast?.(e, n);
		}
		function M(e) {
			t.state.payload = e, t.options.onPayloadChange?.(e);
		}
		async function N(e = {}) {
			if (!(r.value && e.quiet)) {
				e.quiet || (r.value = e.snapshot ? "Refreshing live snapshot…" : "Refreshing dashboard…"), i.value = "";
				try {
					let n = t.options.dashboardEndpoint.includes("?") ? "&" : "?";
					M(await As(e.snapshot ? `${t.options.dashboardEndpoint}${n}refresh_snapshot=true` : t.options.dashboardEndpoint)), e.quiet || j("Dashboard refreshed.");
				} catch (t) {
					i.value = t instanceof Error ? t.message : "Dashboard refresh failed.", e.quiet || j(i.value, "error");
				} finally {
					e.quiet || (r.value = "");
				}
			}
		}
		async function P() {
			r.value = "Generating fresh briefs…", i.value = "";
			try {
				M(await js(t.options.refreshBriefsEndpoint, { brief_id: null })), j("Dashboard brief refresh queued.");
			} catch (e) {
				i.value = e instanceof Error ? e.message : "Brief refresh failed.", j(i.value, "error");
			} finally {
				r.value = "";
			}
		}
		async function F(e, n) {
			r.value = "Saving dashboard controls…", i.value = "";
			try {
				M(await js(t.options.settingsEndpoint, e)), j(n);
			} catch (e) {
				i.value = e instanceof Error ? e.message : "Dashboard settings failed to save.", j(i.value, "error");
			} finally {
				r.value = "";
			}
		}
		function I() {
			t.options.onPreferencesChange?.({
				showMetrics: a.value,
				showMedia: o.value
			});
		}
		function ee() {
			window.clearInterval(s);
			let e = Number(y.value.refresh_interval_seconds || x.value || 0);
			e > 0 && (s = window.setInterval(() => void N({ quiet: !0 }), Math.max(15, e) * 1e3));
		}
		function te() {
			c = 0;
			let e = u.value;
			if (!e) return;
			let t = window.getComputedStyle(e), n = Number.parseFloat(t.gridAutoRows) || 8, r = Number.parseFloat(t.rowGap) || 12, i = Array.from(e.children).filter((e) => e instanceof HTMLElement);
			i.forEach((e) => {
				e.style.gridRowEnd = "auto";
			}), i.forEach((e) => {
				let t = e.getBoundingClientRect().height, i = Math.max(1, Math.ceil((t + r) / (n + r)));
				e.style.gridRowEnd = `span ${i}`;
			});
		}
		function ne() {
			window.cancelAnimationFrame(c), c = window.requestAnimationFrame(te);
		}
		function L() {
			l?.disconnect();
			let e = u.value;
			e && (l = new ResizeObserver(ne), l.observe(e), Array.from(e.children).forEach((e) => l?.observe(e)), ne());
		}
		On([a, o], I), On(() => t.state.payload, () => {
			b.value = T(v.value.person_id), x.value = Number(y.value.refresh_interval_seconds ?? 300), S.value = Number(y.value.brief_refresh_interval_seconds ?? 3600), ee(), un().then(L);
		}, { immediate: !0 }), Er(() => {
			N({ quiet: !0 }), un().then(L);
		}), kr(() => {
			window.clearInterval(s), window.cancelAnimationFrame(c), l?.disconnect();
		});
		let R = [
			[0, "Off"],
			[30, "30 seconds"],
			[60, "1 minute"],
			[300, "5 minutes"],
			[900, "15 minutes"],
			[1800, "30 minutes"],
			[3600, "1 hour"],
			[7200, "2 hours"],
			[14400, "4 hours"]
		], z = [
			[0, "Off"],
			[300, "5 minutes"],
			[900, "15 minutes"],
			[1800, "30 minutes"],
			[3600, "1 hour"],
			[7200, "2 hours"],
			[14400, "4 hours"],
			[21600, "6 hours"],
			[43200, "12 hours"]
		];
		return (e, t) => (q(), J("div", vg, [
			Y("header", yg, [Y("div", null, [
				t[12] ||= Y("span", { class: "tv-eyebrow" }, "Home at a glance", -1),
				t[13] ||= Y("h1", null, "Dashboard", -1),
				Y("p", null, [d.value.generated_at ? (q(), J("span", bg, "Updated " + V(E(d.value.generated_at)), 1)) : (q(), J("span", xg, "Live status, signals, and Tater summaries."))])
			]), Y("div", Sg, [Y("span", { class: B(["tv-live-pill", { busy: !!r.value }]) }, [t[14] ||= Y("i", null, null, -1), X(V(r.value || "Live"), 1)], 2), Y("button", {
				type: "button",
				class: "tv-button",
				onClick: t[0] ||= (e) => n.value = !0
			}, "Controls")])]),
			i.value ? (q(), J("div", Cg, V(i.value), 1)) : Z("", !0),
			C.value?.text ? (q(), J("section", wg, [Y("div", null, [t[15] ||= Y("span", { class: "tv-eyebrow" }, "Today", -1), Y("h2", null, V(C.value.title || "Home Brief"), 1)]), Y("p", null, V(C.value.text), 1)])) : Z("", !0),
			_.value.length ? (q(), J("section", Tg, [Y("div", Eg, [Y("div", null, [t[16] ||= Y("span", { class: "tv-eyebrow" }, "Update watch", -1), Y("h2", null, V(Number(g.value.total || 0) ? `${g.value.total} available` : "Everything current"), 1)]), Y("span", null, V(g.value.summary || "Firmware and Tater Shop surfaces checked."), 1)]), Y("div", Dg, [(q(!0), J(K, null, G(_.value, (e) => (q(), J("button", {
				key: T(e.kind),
				type: "button",
				onClick: (t) => A(e.kind)
			}, [
				Y("span", null, V(e.label || e.kind), 1),
				Y("strong", null, V(e.error ? "Needs check" : Number(e.count || 0) ? `${e.count} available` : "Current"), 1),
				e.items?.length ? (q(), J("small", kg, V(e.items.slice(0, 3).map((e) => e.name || e.id).join(" • ")), 1)) : (q(), J("small", Ag, V(e.error || "No pending updates"), 1))
			], 8, Og))), 128))])])) : Z("", !0),
			w.value?.text ? (q(), J("section", jg, [
				t[17] ||= Y("span", { class: "tv-eyebrow" }, "Tater", -1),
				Y("h2", null, V(w.value.title || "System summary"), 1),
				Y("p", null, V(w.value.text), 1)
			])) : Z("", !0),
			Y("section", {
				ref_key: "sectionGrid",
				ref: u,
				class: "td-section-grid"
			}, [(q(!0), J(K, null, G(p.value, (e) => (q(), J("article", {
				key: T(e.id),
				class: B(["tv-panel td-section", `section-${T(e.id)}`])
			}, [
				Y("header", Mg, [Y("div", null, [
					Y("span", Ng, V(e.id), 1),
					Y("h2", null, V(e.title || e.id), 1),
					Y("p", null, V(e.subtitle), 1)
				]), D(e)?.updated_at ? (q(), J("span", Pg, V(E(D(e)?.updated_at)), 1)) : Z("", !0)]),
				D(e)?.text ? (q(), J("p", Fg, V(D(e)?.text), 1)) : Z("", !0),
				a.value && e.stats?.length ? (q(), J("div", Ig, [(q(!0), J(K, null, G(e.stats, (e) => (q(), J("div", { key: T(e.label) }, [Y("span", null, V(e.label), 1), Y("strong", null, V(e.value ?? "-"), 1)]))), 128))])) : Z("", !0),
				O(e).length ? (q(), J("div", Lg, [(q(!0), J(K, null, G(O(e), (e, t) => (q(), J("article", {
					key: T(e.id || e.title || t),
					class: "td-item"
				}, [o.value && k(e) ? (q(), J("img", {
					key: 0,
					src: k(e),
					alt: T(e.image_alt || e.title || "Dashboard image"),
					loading: "lazy"
				}, null, 8, Rg)) : Z("", !0), Y("div", null, [Y("strong", null, V(e.title || e.name || e.label || "Signal"), 1), Y("span", null, V(e.subtitle || e.when || e.state || e.detail), 1)])]))), 128))])) : Z("", !0)
			], 2))), 128))], 512),
			ga(kl, {
				open: n.value,
				onClose: t[11] ||= (e) => n.value = !1
			}, {
				default: Sn(() => [Y("section", zg, [
					Y("header", null, [t[18] ||= Y("div", null, [Y("span", { class: "tv-eyebrow" }, "Dashboard"), Y("h2", null, "Controls")], -1), Y("button", {
						class: "tv-button",
						type: "button",
						onClick: t[1] ||= (e) => n.value = !1
					}, "Close")]),
					Y("div", Bg, [
						Y("label", Vg, [W(Y("input", {
							"onUpdate:modelValue": t[2] ||= (e) => a.value = e,
							class: "tv-checkbox",
							type: "checkbox"
						}, null, 512), [[cs, a.value]]), t[19] ||= Y("span", null, [Y("strong", null, "Metric pills"), Y("small", null, "Show compact live readings inside each area.")], -1)]),
						Y("label", Hg, [W(Y("input", {
							"onUpdate:modelValue": t[3] ||= (e) => o.value = e,
							class: "tv-checkbox",
							type: "checkbox"
						}, null, 512), [[cs, o.value]]), t[20] ||= Y("span", null, [Y("strong", null, "Media"), Y("small", null, "Show snapshots and satellite images when available.")], -1)]),
						Y("label", null, [t[21] ||= Y("span", null, "Dashboard refresh", -1), W(Y("select", {
							"onUpdate:modelValue": t[4] ||= (e) => x.value = e,
							onChange: t[5] ||= (e) => F({
								refresh_interval_seconds: x.value,
								brief_refresh_interval_seconds: S.value
							}, "Dashboard refresh updated.")
						}, [(q(), J(K, null, G(R, (e) => Y("option", {
							key: e[0],
							value: e[0]
						}, V(e[1]), 9, Ug)), 64))], 544), [[
							ds,
							x.value,
							void 0,
							{ number: !0 }
						]])]),
						Y("label", null, [t[22] ||= Y("span", null, "Brief refresh", -1), W(Y("select", {
							"onUpdate:modelValue": t[6] ||= (e) => S.value = e,
							onChange: t[7] ||= (e) => F({
								refresh_interval_seconds: x.value,
								brief_refresh_interval_seconds: S.value
							}, "Brief refresh updated.")
						}, [(q(), J(K, null, G(z, (e) => Y("option", {
							key: e[0],
							value: e[0]
						}, V(e[1]), 9, Wg)), 64))], 544), [[
							ds,
							S.value,
							void 0,
							{ number: !0 }
						]])]),
						Y("label", null, [t[24] ||= Y("span", null, "Personal profile", -1), W(Y("select", {
							"onUpdate:modelValue": t[8] ||= (e) => b.value = e,
							onChange: t[9] ||= (e) => F({ personal_person_id: b.value || null }, "Personal dashboard profile updated.")
						}, [t[23] ||= Y("option", { value: "" }, "All people", -1), (q(!0), J(K, null, G(v.value.people_options || [], (e) => (q(), J("option", {
							key: T(e.value),
							value: T(e.value)
						}, V(e.label || e.value), 9, Gg))), 128))], 544), [[ds, b.value]])])
					]),
					Y("footer", null, [Y("span", null, V(r.value || i.value), 1), Y("div", null, [Y("button", {
						class: "tv-button",
						type: "button",
						onClick: t[10] ||= (e) => N({ snapshot: !0 })
					}, "Refresh snapshot"), Y("button", {
						class: "tv-button primary",
						type: "button",
						onClick: P
					}, "Generate briefs")])])
				])]),
				_: 1
			}, 8, ["open"])
		]));
	}
}), qg = { class: "tater-vue-surface ti-integrations" }, Jg = { class: "tv-page-heading" }, Yg = { class: "tv-heading-actions" }, Xg = { class: "tv-metrics ti-summary" }, Zg = {
	class: "tv-tabs",
	"aria-label": "Integration sections"
}, Qg = ["onClick"], $g = {
	key: 1,
	class: "ti-manager"
}, e_ = { class: "tv-mini-tabs" }, t_ = ["onClick"], n_ = { key: 0 }, r_ = {
	key: 0,
	class: "tv-notice error"
}, i_ = {
	key: 1,
	class: "ti-card-grid"
}, a_ = { class: "tv-eyebrow" }, o_ = {
	key: 0,
	class: "ti-version"
}, s_ = {
	key: 1,
	class: "ti-tags"
}, c_ = ["onClick"], l_ = { key: 1 }, u_ = ["onClick"], d_ = {
	key: 0,
	class: "tv-empty"
}, f_ = {
	key: 2,
	class: "ti-card-grid"
}, p_ = { class: "tv-eyebrow" }, m_ = { class: "tv-state" }, h_ = ["onClick"], g_ = {
	key: 0,
	class: "tv-empty"
}, __ = {
	key: 3,
	class: "ti-manage-list"
}, v_ = { class: "ti-manage-toolbar" }, y_ = ["disabled"], b_ = { class: "ti-row-actions" }, x_ = ["disabled", "onClick"], S_ = ["onClick"], C_ = {
	key: 1,
	class: "ti-purge"
}, w_ = ["onUpdate:modelValue"], T_ = ["onClick"], E_ = {
	key: 3,
	class: "tv-state good"
}, D_ = {
	key: 4,
	class: "tv-panel ti-repos"
}, O_ = { class: "ti-repo-row builtin" }, k_ = ["onClick"], A_ = { class: "ti-repo-form" }, j_ = {
	key: 2,
	class: "tv-panel ti-browser"
}, M_ = { class: "tv-panel-head" }, N_ = {
	key: 0,
	class: "ti-browser-layout"
}, P_ = ["onClick"], F_ = { class: "ti-device-content" }, I_ = { class: "tv-eyebrow" }, L_ = { class: "tv-state" }, R_ = { class: "ti-tags" }, z_ = {
	key: 1,
	class: "tv-empty"
}, B_ = {
	key: 3,
	class: "ti-rooms"
}, V_ = { class: "tv-panel ti-room-toolbar" }, H_ = { class: "ti-room-grid" }, U_ = { class: "tv-eyebrow" }, W_ = {
	key: 0,
	class: "ti-room-controls"
}, G_ = ["onUpdate:modelValue"], K_ = ["onClick"], q_ = ["value", "onChange"], J_ = ["value"], Y_ = ["value"], X_ = { class: "ti-room-devices" }, Z_ = ["value", "onChange"], Q_ = ["value"], $_ = ["onUpdate:modelValue"], ev = ["onClick"], tv = ["onClick"], nv = {
	key: 0,
	class: "tv-empty compact"
}, rv = {
	key: 4,
	class: "tv-panel ti-activity"
}, iv = { class: "tv-panel-head" }, av = { class: "tv-metrics" }, ov = { class: "ti-event-list" }, sv = { class: "ti-provider" }, cv = { class: "tv-state" }, lv = {
	key: 0,
	class: "tv-empty"
}, uv = { class: "tv-eyebrow" }, dv = { class: "tv-form-grid" }, fv = ["onUpdate:modelValue"], pv = [
	"onUpdate:modelValue",
	"rows",
	"placeholder"
], mv = ["onUpdate:modelValue"], hv = ["value"], gv = [
	"onUpdate:modelValue",
	"type",
	"min",
	"max",
	"step",
	"placeholder"
], _v = {
	key: 0,
	class: "ti-modal-actions"
}, vv = ["onClick"], yv = {
	key: 0,
	class: "tv-button primary",
	type: "submit"
}, bv = /* @__PURE__ */ sr({
	__name: "IntegrationsApp",
	props: {
		state: {},
		options: {}
	},
	setup(e, { expose: t }) {
		let n = e, r = /* @__PURE__ */ U([
			"manager",
			"devices",
			"rooms",
			"runtime"
		].includes(n.options.initialTab || "") ? String(n.options.initialTab) : "manager"), i = /* @__PURE__ */ U("installed"), a = /* @__PURE__ */ U(""), o = /* @__PURE__ */ U(""), s = /* @__PURE__ */ U(""), c = /* @__PURE__ */ U(""), l = /* @__PURE__ */ U(null), u = /* @__PURE__ */ U({}), d = /* @__PURE__ */ U({}), f = /* @__PURE__ */ U(""), p = /* @__PURE__ */ U(""), m = /* @__PURE__ */ U([]), h = /* @__PURE__ */ U(n.state.settings.integration_device_registry || {}), g = /* @__PURE__ */ U(n.state.settings.integration_runtime || {}), _ = /* @__PURE__ */ U({}), v = /* @__PURE__ */ U({}), y = /* @__PURE__ */ U(""), b = /* @__PURE__ */ U({}), x = /* @__PURE__ */ U({}), S = 0, C = !1, w = !1, T = !1, E = Q(() => n.state.settings || {}), D = Q(() => Array.isArray(E.value.integrations) ? E.value.integrations : []), O = Q(() => E.value.integration_shop || {}), k = Q(() => Array.isArray(O.value.installed) ? O.value.installed : []), A = Q(() => Array.isArray(O.value.catalog) ? O.value.catalog.filter((e) => !e.installed) : []), j = Q(() => k.value.filter((e) => e.update_available)), M = Q(() => k.value.filter((e) => e.enabled).length || (k.value.length ? 0 : D.value.length)), N = Q(() => new Map(D.value.map((e) => [R(e.id), e]))), P = Q(() => {
			let e = /* @__PURE__ */ new Set(), t = k.value.map((t) => {
				let n = L(t.id || t.module_key || t.key);
				return e.add(R(n)), {
					id: n,
					shop: t,
					integration: N.value.get(R(n)) || null
				};
			});
			return D.value.forEach((n) => {
				let r = L(n.id);
				r && !e.has(R(r)) && t.push({
					id: r,
					shop: null,
					integration: n
				});
			}), t.sort((e, t) => L(e.integration?.name || e.shop?.name || e.id).localeCompare(L(t.integration?.name || t.shop?.name || t.id)));
		}), F = Q(() => Array.isArray(h.value.categories) ? h.value.categories.filter((e) => Number(e.device_count || 0) > 0) : []), I = Q(() => F.value.find((e) => L(e.id) === c.value) || F.value[0] || null), ee = Q(() => {
			let e = Array.isArray(h.value.rooms) ? h.value.rooms.slice() : [], t = Array.isArray(h.value.room_overrides?.rooms) ? h.value.room_overrides.rooms : [], n = new Set(e.map((e) => L(e.id)));
			return t.forEach((t) => {
				n.has(L(t.id)) || e.push({
					...t,
					devices: [],
					categories: [],
					source: "tater"
				});
			}), e.sort((e, t) => L(e.name).localeCompare(L(t.name)));
		}), te = Q(() => Array.isArray(h.value.room_media_player_options) ? h.value.room_media_player_options : []), ne = Q(() => (Array.isArray(v.value.events) ? v.value.events : []).filter((e) => {
			let t = L(e.kind || e.type).toLowerCase();
			return ![
				"snapshot",
				"poll",
				"heartbeat",
				"runtime_status"
			].some((e) => t.includes(e));
		}).sort((e, t) => Number(t.ts || 0) - Number(e.ts || 0)).slice(0, 40));
		function L(e) {
			return String(e ?? "").trim();
		}
		function R(e) {
			let t = L(e);
			return t === "ecobee_homekit" ? "homekit" : t;
		}
		function z(e) {
			return encodeURIComponent(L(e));
		}
		function re(e) {
			if (e && typeof e == "object") {
				let t = e;
				return L(t.value ?? t.id ?? t.key ?? t.label);
			}
			return L(e);
		}
		function ie(e) {
			if (e && typeof e == "object") {
				let t = e;
				return L(t.label ?? t.name ?? t.title ?? re(t));
			}
			return L(e);
		}
		function ae(e, t = "success") {
			o.value = e, n.options.onToast?.(e, t);
		}
		function oe(e) {
			return L(e.name || e.friendly_name || e.label || e.title || e.id || e.ref || "Device");
		}
		function se(e) {
			return L(e.id || "unassigned") || "unassigned";
		}
		function ce(e) {
			return L(e.id || e.ref);
		}
		function le(e) {
			return L(e.integration_id);
		}
		function ue(e, t) {
			let n = e.values && Object.prototype.hasOwnProperty.call(e.values, t.key) ? e.values[t.key] : t.default ?? "";
			return L(t.type).toLowerCase() === "checkbox" ? typeof n == "string" ? [
				"1",
				"true",
				"yes",
				"on"
			].includes(n.trim().toLowerCase()) : !!n : n;
		}
		function de(e) {
			let t = { ...u.value };
			return (Array.isArray(e.fields) ? e.fields : []).forEach((e) => {
				let n = L(e.key);
				n && L(e.type).toLowerCase() === "number" && (t[n] = Number(t[n] ?? e.default ?? 0));
			}), t;
		}
		function fe(e) {
			return e.payload && typeof e.payload == "object" ? e.payload : {};
		}
		function pe(e) {
			let t = fe(e);
			return L(t.name || t.friendly_name || t.device_name || t.entity_name || t.entity_id || t.ref || e.provider || "Device change");
		}
		function me(e) {
			let t = fe(e);
			return L(t.state ?? t.value ?? t.status ?? t.current_state ?? e.kind ?? "changed").replaceAll("_", " ");
		}
		function he(e) {
			let t = Math.max(0, Date.now() / 1e3 - Number(e || 0));
			return t < 60 ? "now" : t < 3600 ? `${Math.floor(t / 60)}m ago` : t < 86400 ? `${Math.floor(t / 3600)}h ago` : `${Math.floor(t / 86400)}d ago`;
		}
		async function ge(e = !1) {
			e || (a.value = "Refreshing integrations…"), s.value = "";
			try {
				n.state.settings = await As(n.options.endpoints.settings), h.value = n.state.settings.integration_device_registry || h.value, g.value = n.state.settings.integration_runtime || g.value, m.value = Array.isArray(n.state.settings.integration_shop?.repos?.additional) ? n.state.settings.integration_shop.repos.additional.map((e) => ({ ...e })) : [];
			} catch (t) {
				s.value = t instanceof Error ? t.message : "Integration refresh failed.", e || ae(s.value, "error");
			} finally {
				e || (a.value = "");
			}
		}
		async function _e(e, t = "") {
			if (!(e === "remove" && !window.confirm(`Remove ${t}?${d.value[t] ? " Its saved data will also be deleted." : ""}`))) {
				a.value = `${e.replaceAll("-", " ")} ${t || "integrations"}…`, s.value = "";
				try {
					let r = t ? { id: t } : {};
					e === "remove" && (r.purge_redis = !!d.value[t]), ae(L((await js(`${n.options.endpoints.shop}/${e}`, r)).message) || "Integration action completed."), await ge(!0);
				} catch (e) {
					s.value = e instanceof Error ? e.message : "Integration action failed.", ae(s.value, "error");
				} finally {
					a.value = "";
				}
			}
		}
		function ve(e) {
			l.value = e, u.value = Object.fromEntries((Array.isArray(e.fields) ? e.fields : []).map((t) => [L(t.key), ue(e, t)]));
		}
		async function ye() {
			let e = l.value;
			if (e) {
				a.value = `Saving ${oe(e)}…`;
				try {
					await js(`${n.options.endpoints.integrationSettings}/${z(e.id)}/settings`, { settings: de(e) }), ae(`${oe(e)} settings saved.`), l.value = null, await ge(!0);
				} catch (e) {
					ae(e instanceof Error ? e.message : "Settings save failed.", "error");
				} finally {
					a.value = "";
				}
			}
		}
		async function be(e) {
			let t = l.value;
			if (t) {
				a.value = L(e.status || `Running ${e.label || e.id}…`);
				try {
					let r = await js(`${n.options.endpoints.integrationActions}/${z(t.id)}/actions/${z(e.id)}`, { payload: de(t) }), i = r.values && typeof r.values == "object" ? r.values : r, a = new Set((t.fields || []).map((e) => L(e.key)));
					u.value = {
						...u.value,
						...Object.fromEntries(Object.entries(i).filter(([e]) => a.has(e)))
					}, ae(L(r.message) || `${e.label || e.id} complete.`, r.ok === !1 ? "error" : "success");
				} catch (e) {
					ae(e instanceof Error ? e.message : "Integration action failed.", "error");
				} finally {
					a.value = "";
				}
			}
		}
		async function xe() {
			a.value = "Saving integration repositories…";
			try {
				await js(`${n.options.endpoints.shop}/repos`, { repos: m.value }), ae("Integration repositories saved."), await ge(!0);
			} catch (e) {
				ae(e instanceof Error ? e.message : "Repository save failed.", "error");
			} finally {
				a.value = "";
			}
		}
		function H() {
			let e = p.value.trim();
			if (!e) {
				ae("Repo URL is required.", "error");
				return;
			}
			if (m.value.some((t) => L(t.url).toLowerCase() === e.toLowerCase())) {
				ae("That repo is already added.", "error");
				return;
			}
			m.value.push({
				name: f.value.trim(),
				url: e
			}), f.value = "", p.value = "", o.value = "Repo added. Save repositories to apply it.";
		}
		async function Se(e = !1) {
			try {
				let t = await As(e ? n.options.endpoints.rooms : n.options.endpoints.deviceRegistry);
				h.value = t.registry || t, c.value ||= L(F.value[0]?.id), L(h.value.cache?.source) === "building" && Ce();
			} catch (e) {
				ae(e instanceof Error ? e.message : "Device load failed.", "error");
			}
		}
		async function Ce() {
			if (!w) {
				w = !0;
				try {
					for (let e = 0; e < 120 && !T; e += 1) {
						await new Promise((e) => window.setTimeout(e, 500));
						let e = await As(r.value === "rooms" ? n.options.endpoints.rooms : n.options.endpoints.deviceRegistry), t = e.registry || e;
						if (L(t.cache?.source) !== "building") {
							h.value = t, c.value ||= L(F.value[0]?.id);
							return;
						}
					}
				} catch {} finally {
					w = !1;
				}
			}
		}
		async function we(e = !1) {
			if (!C) {
				C = !0, a.value = e ? "Refreshing rooms in background…" : "Refreshing devices in background…";
				try {
					let e = await js(`${n.options.endpoints.systemTasks}/integration_device_registry/run`), t = Number(e.task?.run_count || 0);
					for (let e = 0; e < 120 && !T; e += 1) {
						await new Promise((e) => window.setTimeout(e, 500));
						let e = await As(n.options.endpoints.systemTasks), i = (Array.isArray(e.tasks) ? e.tasks : []).find((e) => L(e.id) === "integration_device_registry");
						if (!(!i || i.running || Number(i.run_count || 0) <= t)) {
							if (L(i.last_error)) throw Error(L(i.last_error));
							await Se(r.value === "rooms"), ae("Integration devices refreshed.");
							return;
						}
					}
					if (!T) throw Error("The integration device refresh is still running. You can follow it in System Tasks.");
				} catch (e) {
					ae(e instanceof Error ? e.message : "Device refresh failed.", "error");
				} finally {
					C = !1, a.value = "";
				}
			}
		}
		async function Te(e, t) {
			a.value = "Saving organization changes…";
			try {
				let r = await js(n.options.endpoints.rooms, {
					action: e,
					payload: t
				});
				h.value = r.registry || r, ae("Organization changes saved.");
			} catch (e) {
				ae(e instanceof Error ? e.message : "Organization update failed.", "error");
			} finally {
				a.value = "";
			}
		}
		async function Ee() {
			let e = y.value.trim();
			e && (await Te("create_room", { name: e }), y.value = "");
		}
		async function De(e) {
			let t = L(b.value[se(e)] || e.name);
			t && await Te("rename_room", {
				room_id: se(e),
				name: t
			});
		}
		async function Oe(e, t) {
			!t || t === "unassigned" ? await Te("clear_device_room", {
				integration_id: le(e),
				device_id: ce(e)
			}) : await Te("assign_device_room", {
				integration_id: le(e),
				device_id: ce(e),
				room_id: t,
				room_name: L(ee.value.find((e) => se(e) === t)?.name)
			});
		}
		async function ke(e) {
			let t = L(x.value[`${le(e)}:${ce(e)}`] || oe(e));
			t && await Te("rename_device", {
				integration_id: le(e),
				device_id: ce(e),
				name: t
			});
		}
		async function Ae(e, t) {
			t ? await Te("set_room_preferred_media_player", {
				room_id: se(e),
				room_name: e.name,
				target: t
			}) : await Te("clear_room_preferred_media_player", { room_id: se(e) });
		}
		async function je(e = !1) {
			e || (a.value = "Refreshing activity…");
			try {
				let [e, t, r] = await Promise.all([
					As(n.options.endpoints.runtime),
					As(n.options.endpoints.runtimeStates),
					As(`${n.options.endpoints.runtimeEvents}?limit=1000`)
				]);
				g.value = e.runtime || e, _.value = t, v.value = r;
			} catch (t) {
				e || ae(t instanceof Error ? t.message : "Activity refresh failed.", "error");
			} finally {
				e || (a.value = "");
			}
		}
		function Me(e) {
			r.value = e, n.options.onTabChange?.(e);
		}
		return t({ select: Me }), On(() => n.state.settings, () => {
			h.value = n.state.settings.integration_device_registry || h.value, g.value = n.state.settings.integration_runtime || g.value;
		}), On(F, (e) => {
			e.some((e) => L(e.id) === c.value) || (c.value = L(e[0]?.id));
		}, { immediate: !0 }), On(ee, (e) => {
			let t = {}, n = {};
			e.forEach((e) => {
				t[se(e)] = L(e.name), (e.devices || []).forEach((e) => {
					n[`${le(e)}:${ce(e)}`] = oe(e);
				});
			}), b.value = t, x.value = n;
		}, { immediate: !0 }), On(r, (e) => {
			window.clearInterval(S), e === "devices" && Se(!1), e === "rooms" && Se(!0), e === "runtime" && (je(), S = window.setInterval(() => void je(!0), 1e4));
		}, { immediate: !0 }), kr(() => {
			T = !0, window.clearInterval(S);
		}), m.value = Array.isArray(O.value.repos?.additional) ? O.value.repos.additional.map((e) => ({ ...e })) : [], (e, t) => (q(), J("div", qg, [
			Y("header", Jg, [t[11] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Connected home"),
				Y("h1", null, "Integrations"),
				Y("p", null, "Services, devices, rooms, live state, and Tater Shop updates in one place.")
			], -1), Y("div", Yg, [Y("span", { class: B(["tv-live-pill", { busy: !!a.value }]) }, [t[10] ||= Y("i", null, null, -1), X(V(a.value || "Live"), 1)], 2), Y("button", {
				class: "tv-button",
				type: "button",
				onClick: t[0] ||= (e) => ge()
			}, "Refresh")])]),
			Y("div", Xg, [
				Y("div", null, [t[12] ||= Y("span", null, "Installed", -1), Y("strong", null, V(k.value.length || D.value.length), 1)]),
				Y("div", null, [t[13] ||= Y("span", null, "Enabled", -1), Y("strong", null, V(M.value), 1)]),
				Y("div", null, [t[14] ||= Y("span", null, "Devices", -1), Y("strong", null, V(Number(h.value.total || 0)), 1)]),
				Y("div", null, [t[15] ||= Y("span", null, "Updates", -1), Y("strong", null, V(Number(O.value.updates_available || j.value.length)), 1)])
			]),
			o.value || s.value ? (q(), J("div", {
				key: 0,
				class: B(["tv-notice", { error: !!s.value }])
			}, V(s.value || o.value), 3)) : Z("", !0),
			Y("nav", Zg, [(q(), J(K, null, G([
				{
					id: "manager",
					label: "Manager"
				},
				{
					id: "devices",
					label: "Devices"
				},
				{
					id: "rooms",
					label: "Organize"
				},
				{
					id: "runtime",
					label: "Activity"
				}
			], (e) => Y("button", {
				key: e.id,
				type: "button",
				class: B({ active: r.value === e.id }),
				onClick: (t) => Me(e.id)
			}, V(e.label), 11, Qg)), 64))]),
			r.value === "manager" ? (q(), J("section", $g, [
				Y("nav", e_, [(q(), J(K, null, G([
					{
						id: "installed",
						label: "Installed"
					},
					{
						id: "store",
						label: "Store"
					},
					{
						id: "manage",
						label: "Manage"
					},
					{
						id: "repos",
						label: "Repositories"
					}
				], (e) => Y("button", {
					key: e.id,
					class: B({ active: i.value === e.id }),
					type: "button",
					onClick: (t) => i.value = e.id
				}, [X(V(e.label), 1), e.id === "manage" && j.value.length ? (q(), J("span", n_, V(j.value.length), 1)) : Z("", !0)], 10, t_)), 64))]),
				O.value.errors?.length ? (q(), J("div", r_, V(O.value.errors.join(" • ")), 1)) : Z("", !0),
				i.value === "installed" ? (q(), J("div", i_, [(q(!0), J(K, null, G(P.value, (e) => (q(), J("article", {
					key: e.id,
					class: "tv-panel ti-integration-card"
				}, [
					Y("header", null, [Y("div", null, [Y("span", a_, V(e.id), 1), Y("h2", null, V(e.integration?.name || e.shop?.name || e.id), 1)]), Y("span", { class: B(["tv-state", { good: e.shop?.enabled !== !1 }]) }, V(e.shop?.enabled === !1 ? "Disabled" : "Enabled"), 3)]),
					Y("p", null, V(e.integration?.description || e.shop?.description || "Connected integration."), 1),
					e.shop ? (q(), J("div", o_, [X("Installed " + V(e.shop.installed_ver || "0.0.0") + " ", 1), Y("span", null, "Store " + V(e.shop.store_ver || "-"), 1)])) : Z("", !0),
					e.integration?.capabilities?.length ? (q(), J("div", s_, [(q(!0), J(K, null, G(e.integration.capabilities, (e) => (q(), J("span", { key: e }, V(e), 1))), 128))])) : Z("", !0),
					Y("footer", null, [e.integration && (e.integration.fields?.length || e.integration.actions?.length) ? (q(), J("button", {
						key: 0,
						class: "tv-button",
						type: "button",
						onClick: (t) => ve(e.integration)
					}, "Settings", 8, c_)) : (q(), J("span", l_, "No configurable settings")), e.shop && !e.shop.required ? (q(), J("button", {
						key: 2,
						class: "tv-button",
						type: "button",
						onClick: (t) => _e(e.shop.enabled ? "disable" : "enable", e.id)
					}, V(e.shop.enabled ? "Disable" : "Enable"), 9, u_)) : Z("", !0)])
				]))), 128)), P.value.length ? Z("", !0) : (q(), J("div", d_, "No installed integrations found."))])) : i.value === "store" ? (q(), J("div", f_, [(q(!0), J(K, null, G(A.value, (e) => (q(), J("article", {
					key: e.id,
					class: "tv-panel ti-integration-card"
				}, [
					Y("header", null, [Y("div", null, [Y("span", p_, V(e.id), 1), Y("h2", null, V(e.name || e.id), 1)]), Y("span", m_, "v" + V(e.version || "-"), 1)]),
					Y("p", null, V(e.description), 1),
					Y("footer", null, [Y("span", null, V(e.source_label || "Tater Shop"), 1), Y("button", {
						class: "tv-button primary",
						type: "button",
						onClick: (t) => _e("install", e.id)
					}, "Download", 8, h_)])
				]))), 128)), A.value.length ? Z("", !0) : (q(), J("div", g_, "No additional integrations are available."))])) : i.value === "manage" ? (q(), J("div", __, [Y("div", v_, [Y("div", null, [t[16] ||= Y("h2", null, "Manage installed integrations", -1), Y("p", null, V(j.value.length) + " update" + V(j.value.length === 1 ? "" : "s") + " available.", 1)]), Y("button", {
					class: "tv-button primary",
					type: "button",
					disabled: !j.value.length,
					onClick: t[1] ||= (e) => _e("update-all")
				}, "Update all", 8, y_)]), (q(!0), J(K, null, G(k.value, (e) => (q(), J("article", {
					key: e.id,
					class: "tv-panel ti-manage-row"
				}, [Y("div", null, [Y("strong", null, V(e.name || e.id), 1), Y("span", null, V(e.installed_ver || "0.0.0") + " → " + V(e.store_ver || "-"), 1)]), Y("div", b_, [
					Y("button", {
						class: "tv-button",
						type: "button",
						disabled: !e.update_available,
						onClick: (t) => _e("update", e.id)
					}, V(e.update_available ? "Update" : "Current"), 9, x_),
					e.required ? Z("", !0) : (q(), J("button", {
						key: 0,
						class: "tv-button",
						type: "button",
						onClick: (t) => _e(e.enabled ? "disable" : "enable", e.id)
					}, V(e.enabled ? "Disable" : "Enable"), 9, S_)),
					e.required ? Z("", !0) : (q(), J("label", C_, [W(Y("input", {
						"onUpdate:modelValue": (t) => d.value[e.id] = t,
						type: "checkbox"
					}, null, 8, w_), [[cs, d.value[e.id]]]), t[17] ||= X(" Delete data", -1)])),
					e.required ? (q(), J("span", E_, "Required")) : (q(), J("button", {
						key: 2,
						class: "tv-button danger",
						type: "button",
						onClick: (t) => _e("remove", e.id)
					}, "Remove", 8, T_))
				])]))), 128))])) : (q(), J("div", D_, [
					t[21] ||= Y("header", null, [Y("div", null, [
						Y("span", { class: "tv-eyebrow" }, "Sources"),
						Y("h2", null, "Integration repositories"),
						Y("p", null, "The built-in repository stays available; add trusted sources below.")
					])], -1),
					Y("article", O_, [Y("div", null, [Y("strong", null, V(O.value.repos?.default?.name || "Default"), 1), Y("code", null, V(O.value.repos?.default?.url || "(not set)"), 1)]), t[18] ||= Y("span", null, "Built-in", -1)]),
					(q(!0), J(K, null, G(m.value, (e, t) => (q(), J("article", {
						key: `${e.url}-${t}`,
						class: "ti-repo-row"
					}, [Y("div", null, [Y("strong", null, V(e.name || "Additional repo"), 1), Y("code", null, V(e.url), 1)]), Y("button", {
						class: "tv-button",
						type: "button",
						onClick: (e) => m.value.splice(t, 1)
					}, "Remove", 8, k_)]))), 128)),
					Y("div", A_, [
						Y("label", null, [t[19] ||= Y("span", null, "Name (optional)", -1), W(Y("input", {
							"onUpdate:modelValue": t[2] ||= (e) => f.value = e,
							type: "text",
							placeholder: "My Integration Repo"
						}, null, 512), [[$, f.value]])]),
						Y("label", null, [t[20] ||= Y("span", null, "Repo URL", -1), W(Y("input", {
							"onUpdate:modelValue": t[3] ||= (e) => p.value = e,
							type: "url",
							placeholder: "https://example.com/integrations.json",
							onKeyup: Ss(H, ["enter"])
						}, null, 544), [[$, p.value]])]),
						Y("button", {
							class: "tv-button",
							type: "button",
							onClick: H
						}, "Add"),
						Y("button", {
							class: "tv-button primary",
							type: "button",
							onClick: xe
						}, "Save repositories")
					])
				]))
			])) : r.value === "devices" ? (q(), J("section", j_, [Y("header", M_, [t[22] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Device registry"),
				Y("h2", null, "Browse devices"),
				Y("p", null, "Grouped by category, room, and integration.")
			], -1), Y("button", {
				class: "tv-button",
				type: "button",
				onClick: t[4] ||= (e) => we(!1)
			}, "Refresh devices")]), F.value.length ? (q(), J("div", N_, [Y("aside", null, [(q(!0), J(K, null, G(F.value, (e) => (q(), J("button", {
				key: e.id,
				type: "button",
				class: B({ active: I.value?.id === e.id }),
				onClick: (t) => c.value = L(e.id)
			}, [Y("strong", null, V(e.name), 1), Y("span", null, V(e.device_count) + " devices · " + V(e.room_count) + " rooms", 1)], 10, P_))), 128))]), Y("div", F_, [Y("header", null, [Y("div", null, [
				Y("span", I_, V(I.value?.id), 1),
				Y("h2", null, V(I.value?.name), 1),
				Y("p", null, V(I.value?.description), 1)
			])]), (q(!0), J(K, null, G(I.value?.rooms || [], (e) => (q(), J("div", {
				key: e.id,
				class: "ti-device-room"
			}, [Y("div", null, [Y("strong", null, V(e.name), 1), Y("span", null, V(e.devices?.length || 0) + " devices", 1)]), (q(!0), J(K, null, G(e.devices || [], (e) => (q(), J("article", {
				key: ce(e),
				class: "ti-device-row"
			}, [
				Y("div", null, [Y("strong", null, V(oe(e)), 1), Y("span", null, V([
					e.integration_name || e.integration_id,
					e.type,
					e.ref || e.id
				].filter(Boolean).join(" / ")), 1)]),
				Y("div", null, [Y("span", L_, V(e.state || e.status || "unknown"), 1), Y("small", null, V(e.room || e.area || "Unassigned"), 1)]),
				Y("div", R_, [(q(!0), J(K, null, G((e.features?.length ? e.features : e.actions || e.capabilities || []).slice(0, 6), (e) => (q(), J("span", { key: e }, V(L(e).replaceAll("_", " ")), 1))), 128))])
			]))), 128))]))), 128))])])) : (q(), J("div", z_, "No devices are available from enabled integrations yet."))])) : r.value === "rooms" ? (q(), J("section", B_, [Y("div", V_, [t[23] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Organization"),
				Y("h2", null, "Rooms and device names"),
				Y("p", null, "Set Tater-friendly names, room assignments, and preferred playback targets.")
			], -1), Y("div", null, [
				W(Y("input", {
					"onUpdate:modelValue": t[5] ||= (e) => y.value = e,
					type: "text",
					placeholder: "New room name",
					onKeyup: Ss(Ee, ["enter"])
				}, null, 544), [[$, y.value]]),
				Y("button", {
					class: "tv-button primary",
					type: "button",
					onClick: Ee
				}, "Create room"),
				Y("button", {
					class: "tv-button",
					type: "button",
					onClick: t[6] ||= (e) => we(!0)
				}, "Refresh")
			])]), Y("div", H_, [(q(!0), J(K, null, G(ee.value, (e) => (q(), J("article", {
				key: se(e),
				class: "tv-panel ti-room-card"
			}, [
				Y("header", null, [Y("div", null, [Y("span", U_, V(e.source || "integration"), 1), Y("h2", null, V(e.name || "Unassigned"), 1)]), Y("span", null, V(e.devices?.length || 0) + " devices", 1)]),
				se(e) === "unassigned" ? Z("", !0) : (q(), J("div", W_, [Y("label", null, [t[24] ||= Y("span", null, "Room name", -1), Y("div", null, [W(Y("input", {
					"onUpdate:modelValue": (t) => b.value[se(e)] = t,
					type: "text"
				}, null, 8, G_), [[$, b.value[se(e)]]]), Y("button", {
					class: "tv-button",
					type: "button",
					onClick: (t) => De(e)
				}, "Rename", 8, K_)])]), Y("label", null, [t[26] ||= Y("span", null, "Preferred player", -1), Y("select", {
					value: e.preferred_media_player || "",
					onChange: (t) => Ae(e, t.target.value)
				}, [
					t[25] ||= Y("option", { value: "" }, "Auto", -1),
					e.preferred_media_player && !te.value.some((t) => L(t.value) === L(e.preferred_media_player)) ? (q(), J("option", {
						key: 0,
						value: e.preferred_media_player
					}, V(e.preferred_media_player) + " (saved)", 9, J_)) : Z("", !0),
					(q(!0), J(K, null, G(te.value, (e) => (q(), J("option", {
						key: e.value,
						value: e.value
					}, V(e.label || e.value), 9, Y_))), 128))
				], 40, q_)])])),
				Y("div", X_, [(q(!0), J(K, null, G(e.devices || [], (n) => (q(), J("article", { key: `${le(n)}:${ce(n)}` }, [
					Y("div", null, [Y("strong", null, V(oe(n)), 1), Y("span", null, V(n.integration_name || n.integration_id) + " · " + V(n.type || "device"), 1)]),
					Y("label", null, [t[28] ||= Y("span", null, "Room", -1), Y("select", {
						value: n.room_id || se(e),
						onChange: (e) => Oe(n, e.target.value)
					}, [t[27] ||= Y("option", { value: "unassigned" }, "Unassigned", -1), (q(!0), J(K, null, G(ee.value.filter((e) => se(e) !== "unassigned"), (e) => (q(), J("option", {
						key: se(e),
						value: se(e)
					}, V(e.name), 9, Q_))), 128))], 40, Z_)]),
					Y("label", null, [t[29] ||= Y("span", null, "Tater name", -1), Y("div", null, [
						W(Y("input", {
							"onUpdate:modelValue": (e) => x.value[`${le(n)}:${ce(n)}`] = e,
							type: "text"
						}, null, 8, $_), [[$, x.value[`${le(n)}:${ce(n)}`]]]),
						Y("button", {
							class: "tv-button",
							type: "button",
							onClick: (e) => ke(n)
						}, "Save", 8, ev),
						n.device_name_source === "tater_override" ? (q(), J("button", {
							key: 0,
							class: "tv-button",
							type: "button",
							onClick: (e) => Te("clear_device_name", {
								integration_id: le(n),
								device_id: ce(n)
							})
						}, "Use integration", 8, tv)) : Z("", !0)
					])])
				]))), 128)), e.devices?.length ? Z("", !0) : (q(), J("div", nv, "No devices assigned."))])
			]))), 128))])])) : (q(), J("section", rv, [
				Y("header", iv, [t[30] ||= Y("div", null, [
					Y("span", { class: "tv-eyebrow" }, "Live integrations"),
					Y("h2", null, "Activity"),
					Y("p", null, "Connection health and recent device-level changes.")
				], -1), Y("button", {
					class: "tv-button",
					type: "button",
					onClick: t[7] ||= (e) => je()
				}, "Refresh")]),
				Y("div", av, [
					(q(!0), J(K, null, G(g.value.enabled_integrations || [], (e) => (q(), J("div", { key: e }, [Y("span", null, V(L(e).replaceAll("_", " ")), 1), Y("strong", null, V(g.value[`${e}_ws_connected`] || g.value[`${e}_connected`] ? "Connected" : "Enabled"), 1)]))), 128)),
					Y("div", null, [t[31] ||= Y("span", null, "Events", -1), Y("strong", null, V(g.value.last_event_seq || 0), 1)]),
					Y("div", null, [t[32] ||= Y("span", null, "Tracked states", -1), Y("strong", null, V(_.value.count || g.value.state_count || 0), 1)])
				]),
				Y("div", ov, [(q(!0), J(K, null, G(ne.value, (e) => (q(), J("article", { key: e.seq }, [
					Y("span", sv, V(L(e.provider).replaceAll("_", " ")), 1),
					Y("div", null, [Y("strong", null, V(pe(e)), 1), Y("small", null, V(fe(e).room || fe(e).area || fe(e).entity_id || fe(e).ref || ""), 1)]),
					Y("span", cv, V(me(e)), 1),
					Y("time", null, V(he(e.ts)), 1)
				]))), 128)), ne.value.length ? Z("", !0) : (q(), J("div", lv, "No recent device changes in the current activity window."))])
			])),
			ga(kl, {
				open: !!l.value,
				onClose: t[9] ||= (e) => l.value = null
			}, {
				default: Sn(() => [Y("form", {
					class: "tv-modal",
					onSubmit: bs(ye, ["prevent"])
				}, [
					Y("header", null, [Y("div", null, [Y("span", uv, V(l.value?.id), 1), Y("h2", null, V(l.value ? oe(l.value) : "") + " settings", 1)]), Y("button", {
						class: "tv-button",
						type: "button",
						onClick: t[8] ||= (e) => l.value = null
					}, "Close")]),
					Y("div", dv, [(q(!0), J(K, null, G(l.value?.fields || [], (e) => (q(), J("label", {
						key: e.key,
						class: B({ full: e.full_width || e.type === "textarea" })
					}, [
						Y("span", null, V(e.label || e.key), 1),
						e.type === "checkbox" ? W((q(), J("input", {
							key: 0,
							"onUpdate:modelValue": (t) => u.value[e.key] = t,
							class: "tv-checkbox",
							type: "checkbox"
						}, null, 8, fv)), [[cs, u.value[e.key]]]) : e.type === "textarea" ? W((q(), J("textarea", {
							key: 1,
							"onUpdate:modelValue": (t) => u.value[e.key] = t,
							rows: e.rows || 3,
							placeholder: e.placeholder
						}, null, 8, pv)), [[$, u.value[e.key]]]) : e.type === "select" ? W((q(), J("select", {
							key: 2,
							"onUpdate:modelValue": (t) => u.value[e.key] = t
						}, [(q(!0), J(K, null, G(e.options || [], (e) => (q(), J("option", {
							key: re(e),
							value: re(e)
						}, V(ie(e)), 9, hv))), 128))], 8, mv)), [[ds, u.value[e.key]]]) : W((q(), J("input", {
							key: 3,
							"onUpdate:modelValue": (t) => u.value[e.key] = t,
							type: [
								"password",
								"number",
								"email",
								"url"
							].includes(e.type) ? e.type : "text",
							min: e.min,
							max: e.max,
							step: e.step,
							placeholder: e.placeholder
						}, null, 8, gv)), [[hs, u.value[e.key]]]),
						Y("small", null, V(e.description), 1)
					], 2))), 128))]),
					l.value?.actions?.length ? (q(), J("div", _v, [t[33] ||= Y("span", null, "Actions", -1), (q(!0), J(K, null, G(l.value?.actions || [], (e) => (q(), J("button", {
						key: e.id,
						class: "tv-button",
						type: "button",
						onClick: (t) => be(e)
					}, V(e.label || e.id), 9, vv))), 128))])) : Z("", !0),
					Y("footer", null, [Y("span", null, V(a.value || o.value), 1), l.value?.fields?.length ? (q(), J("button", yv, "Save settings")) : Z("", !0)])
				], 32)]),
				_: 1
			}, 8, ["open"])
		]));
	}
}), xv = { class: "tater-vue-surface tp-portals" }, Sv = { class: "tv-page-heading" }, Cv = { class: "tv-heading-actions" }, wv = { class: "tv-metrics" }, Tv = {
	key: 1,
	class: "tv-notice error"
}, Ev = {
	class: "tv-tabs tp-tabs",
	"aria-label": "Portal sections"
}, Dv = ["onClick"], Ov = { key: 0 }, kv = {
	key: 2,
	class: "tp-card-grid"
}, Av = { class: "tv-eyebrow" }, jv = { class: "tp-version" }, Mv = ["onClick"], Nv = { key: 1 }, Pv = ["onClick"], Fv = {
	key: 0,
	class: "tv-empty"
}, Iv = {
	key: 3,
	class: "tp-card-grid"
}, Lv = { class: "tv-eyebrow" }, Rv = { class: "tv-state" }, zv = ["onClick"], Bv = {
	key: 0,
	class: "tv-empty"
}, Vv = {
	key: 4,
	class: "tp-manage-list tv-manage-workspace"
}, Hv = { class: "tv-panel tp-manage-toolbar tv-manage-hero" }, Uv = { class: "tv-manage-overview" }, Wv = ["disabled"], Gv = { class: "tv-manage-identity" }, Kv = { class: "tv-manage-monogram" }, qv = { class: "tv-eyebrow" }, Jv = { class: "tv-manage-version" }, Yv = { class: "tv-manage-actions" }, Xv = ["disabled", "onClick"], Zv = ["onClick"], Qv = { class: "ti-purge" }, $v = ["onUpdate:modelValue"], ey = ["onClick"], ty = {
	key: 0,
	class: "tv-empty"
}, ny = {
	key: 5,
	class: "tv-panel tp-repos tv-repository-manager"
}, ry = {
	class: "tv-repository-tabs",
	"aria-label": "Portal repository sources"
}, iy = {
	key: 0,
	class: "tv-trusted-repositories"
}, ay = {
	key: 0,
	class: "tv-repository-warning"
}, oy = { class: "tv-trusted-repo-grid" }, sy = {
	class: "tv-trusted-repo-card builtin selected",
	"aria-label": "Built-in Tater Portal Shop repository"
}, cy = { class: "tv-repo-card-top" }, ly = [
	"aria-pressed",
	"onClick",
	"onKeydown"
], uy = { class: "tv-repo-check" }, dy = { class: "tv-repo-card-top" }, fy = { class: "tv-repo-monogram" }, py = ["href"], my = { key: 1 }, hy = {
	key: 0,
	class: "ti-tags"
}, gy = ["href"], _y = {
	key: 1,
	class: "tv-empty compact"
}, vy = {
	key: 1,
	class: "tv-custom-repositories"
}, yy = ["onClick"], by = {
	key: 0,
	class: "tv-empty compact"
}, xy = { class: "tp-repo-form" }, Sy = { class: "tv-eyebrow" }, Cy = {
	key: 0,
	class: "tv-notice error"
}, wy = {
	key: 1,
	class: "tv-empty compact"
}, Ty = {
	key: 2,
	class: "tvb-field-grid"
}, Ey = ["disabled"], Dy = /* @__PURE__ */ sr({
	__name: "PortalsApp",
	props: {
		state: {},
		options: {}
	},
	setup(e, { expose: t }) {
		let n = e, r = [
			{
				id: "installed",
				label: "Installed"
			},
			{
				id: "store",
				label: "Store"
			},
			{
				id: "manage",
				label: "Manage"
			},
			{
				id: "repos",
				label: "Repositories"
			}
		], i = /* @__PURE__ */ U(r.some((e) => e.id === n.options.initialTab) ? String(n.options.initialTab) : "installed"), a = (e) => {
			r.some((t) => t.id === e) && (i.value = e);
		}, o = /* @__PURE__ */ U(""), s = /* @__PURE__ */ U(""), c = /* @__PURE__ */ U(""), l = /* @__PURE__ */ U({}), u = /* @__PURE__ */ U(""), d = /* @__PURE__ */ U(""), f = /* @__PURE__ */ U([]), p = /* @__PURE__ */ U("trusted"), m = /* @__PURE__ */ U(null), h = /* @__PURE__ */ U({}), g = /* @__PURE__ */ U(!1), _ = /* @__PURE__ */ U(""), v = 0, y = Q(() => n.state.payload?.runtime || {}), b = Q(() => n.state.payload?.shop || {}), x = Q(() => Array.isArray(y.value.items) ? y.value.items : []), S = Q(() => Array.isArray(b.value.installed) ? b.value.installed : []), C = Q(() => Array.isArray(b.value.catalog) ? b.value.catalog : []), w = Q(() => C.value.filter((e) => !e.installed).sort(F)), T = Q(() => S.value.filter((e) => e.update_available)), E = Q(() => x.value.filter((e) => !!e.running).length), D = Q(() => Array.isArray(b.value.repos?.trusted) ? b.value.repos.trusted : []), O = Q(() => new Map(x.value.map((e) => [N(e.key), e]))), k = Q(() => {
			let e = /* @__PURE__ */ new Map();
			return S.value.forEach((t) => {
				let n = j(t.module_key || `${t.id}_portal`);
				n && e.set(N(n), t), t.id && e.set(N(t.id), t);
			}), e;
		}), A = Q(() => {
			let e = /* @__PURE__ */ new Set(), t = x.value.map((t) => {
				let n = j(t.key), r = k.value.get(N(n)) || k.value.get(N(P(n))) || null;
				return r && e.add(N(r.id)), {
					key: n,
					runtime: t,
					shop: r
				};
			});
			return S.value.forEach((n) => {
				e.has(N(n.id)) || t.push({
					key: j(n.module_key || `${n.id}_portal`),
					runtime: null,
					shop: n
				});
			}), t.sort((e, t) => I(e).localeCompare(I(t), void 0, {
				sensitivity: "base",
				numeric: !0
			}));
		});
		function j(e) {
			return String(e ?? "").trim();
		}
		function M(e) {
			return encodeURIComponent(j(e));
		}
		function N(e) {
			return j(e).toLowerCase();
		}
		function P(e) {
			return j(e).replace(/_portal$/i, "");
		}
		function F(e, t) {
			return j(e.name || e.id).localeCompare(j(t.name || t.id), void 0, {
				sensitivity: "base",
				numeric: !0
			});
		}
		function I(e) {
			return j(e.runtime?.label || e.shop?.name || P(e.key));
		}
		function ee(e) {
			return j(e.shop?.description || "Local Portal module.");
		}
		function te(e) {
			let t = j(e.module_key || `${e.id}_portal`);
			return O.value.get(N(t)) || O.value.get(N(e.id)) || null;
		}
		function ne(e) {
			return e ? e.running ? "Running" : e.desired_running ? "Pending start" : "Stopped" : "Unavailable";
		}
		function L(e, t = "success") {
			s.value = e, c.value = t === "error" ? e : "", n.options.onToast?.(e, t);
		}
		function R() {
			let e = new Set(D.value.map((e) => j(e.url).toLowerCase()).filter(Boolean));
			f.value = Array.isArray(b.value.repos?.additional) ? b.value.repos.additional.filter((t) => !e.has(j(t.url).toLowerCase())).map((e) => ({ ...e })) : [];
		}
		function z() {
			return D.value.filter((e) => !!e.enabled).map((e) => ({
				name: j(e.name),
				url: j(e.url)
			}));
		}
		function re() {
			return [...z(), ...f.value];
		}
		async function ie(e) {
			if (o.value) return;
			let t = !!e.enabled, r = !e.enabled;
			e.enabled = r, o.value = `${r ? "Adding" : "Removing"} ${j(e.name || e.repository)}…`;
			try {
				await js(`${n.options.endpoints.shop}/repos`, { repos: re() }), L(`${j(e.name || e.repository)} ${r ? "added to" : "removed from"} the Portal Store.`), await ae(!0);
			} catch (n) {
				e.enabled = t, L(n instanceof Error ? n.message : "Trusted repository update failed.", "error");
			} finally {
				o.value = "";
			}
		}
		async function ae(e = !1) {
			e || (o.value = "Refreshing Portals…"), c.value = "";
			try {
				let [e, t] = await Promise.all([As(n.options.endpoints.runtime), As(n.options.endpoints.shop)]);
				n.state.payload = {
					runtime: e,
					shop: t
				}, R();
			} catch (e) {
				L(e instanceof Error ? e.message : "Portal refresh failed.", "error");
			} finally {
				e || (o.value = "");
			}
		}
		async function oe(e, t) {
			let r = j(e.key);
			if (r) {
				o.value = `${t === "start" ? "Starting" : "Stopping"} ${r}…`;
				try {
					await js(`${n.options.endpoints.runtime}/${M(r)}/${t}`), L(`${r} ${t === "start" ? "started" : "stopped"}.`), await ae(!0), n.options.onHealthRefresh?.();
				} catch (e) {
					L(e instanceof Error ? e.message : `Portal ${t} failed.`, "error");
				} finally {
					o.value = "";
				}
			}
		}
		async function se(e, t = "") {
			if (!(e === "remove" && !window.confirm(`Remove ${t}?${l.value[t] ? " Its saved data will also be deleted." : ""}`))) {
				o.value = `${e.replaceAll("-", " ")} ${t || "Portals"}…`, c.value = "";
				try {
					let r = t ? { id: t } : {};
					e === "remove" && (r.purge_redis = !!l.value[t]);
					let a = await js(`${n.options.endpoints.shop}/${e}`, r), o = Array.isArray(a.updated) ? a.updated.length : 0, s = Array.isArray(a.failed) ? a.failed.length : 0, c = e === "update-all" ? `Update-all completed. Updated ${o}, failed ${s}.` : "Portal action completed.";
					L(j(a.message) || c, s ? "error" : "success"), await ae(!0), e === "install" && (i.value = "installed"), n.options.onHealthRefresh?.();
				} catch (e) {
					L(e instanceof Error ? e.message : "Portal action failed.", "error");
				} finally {
					o.value = "";
				}
			}
		}
		function ce(e) {
			let t = e.value ?? e.default ?? "", n = j(e.type).toLowerCase();
			if (n === "checkbox") return typeof t == "string" ? [
				"1",
				"true",
				"yes",
				"on",
				"enabled"
			].includes(t.toLowerCase()) : !!t;
			if (n === "number" || n === "range") return t === "" ? "" : Number(t);
			if (n === "multiselect") {
				if (Array.isArray(t)) return [...t];
				let e = j(t);
				if (!e) return [];
				try {
					let t = JSON.parse(e);
					if (Array.isArray(t)) return t;
				} catch {}
				return e.split(",").map((e) => e.trim()).filter(Boolean);
			}
			return t;
		}
		function le(e) {
			return (Array.isArray(e.show_when_all) ? e.show_when_all : e.show_when && typeof e.show_when == "object" ? [e.show_when] : []).every((e) => {
				let t = j(e.source_key ?? e.key);
				if (!t) return !0;
				let n = [
					...e.any_of || [],
					...e.values || [],
					...e.equals === void 0 ? [] : [e.equals],
					...e.value === void 0 ? [] : [e.value]
				].map((e) => String(e ?? "").trim());
				if (!n.length) return !0;
				let r = typeof h.value[t] == "boolean" ? h.value[t] ? "true" : "false" : String(h.value[t] ?? "").trim();
				return n.includes(r);
			});
		}
		function ue(e) {
			h.value = Object.fromEntries((Array.isArray(e.settings) ? e.settings : []).filter((e) => j(e.key)).map((e) => [j(e.key), ce(e)]));
		}
		function de() {
			v += 1, g.value = !1, _.value = "", m.value = null;
		}
		async function fe(e) {
			let t = j(e.key);
			if (!t) return;
			let r = ++v;
			m.value = { ...e }, ue(e), g.value = !0, _.value = "";
			try {
				let i = await As(`${n.options.endpoints.runtime}/${M(t)}/settings`);
				if (r !== v || j(m.value?.key) !== t) return;
				let a = {
					...e,
					...i,
					settings: Array.isArray(i.settings) ? i.settings : e.settings
				};
				m.value = a, ue(a);
			} catch (e) {
				if (r !== v || j(m.value?.key) !== t) return;
				_.value = e instanceof Error ? e.message : "Live Portal settings could not be loaded.";
			} finally {
				r === v && (g.value = !1);
			}
		}
		async function pe() {
			let e = m.value;
			if (!e) return;
			let t = j(e.key);
			o.value = `Saving ${j(e.label || t)}…`;
			try {
				let r = Object.fromEntries((e.settings || []).filter((e) => {
					let t = j(e.type).toLowerCase();
					return j(e.key) && ![
						"section",
						"header",
						"readonly",
						"read_only",
						"led_preview"
					].includes(t) && le(e);
				}).map((e) => [j(e.key), h.value[j(e.key)]]));
				await js(`${n.options.endpoints.runtime}/${M(t)}/settings`, { values: r }), L(`Saved settings for ${j(e.label || t)}.`), de(), await ae(!0);
			} catch (e) {
				L(e instanceof Error ? e.message : "Portal settings save failed.", "error");
			} finally {
				o.value = "";
			}
		}
		function me() {
			let e = d.value.trim();
			if (!e) {
				L("Repository URL is required.", "error");
				return;
			}
			if ([...f.value, ...D.value].some((t) => j(t.url).toLowerCase() === e.toLowerCase())) {
				L("That repository is already listed.", "error");
				return;
			}
			f.value.push({
				name: u.value.trim(),
				url: e
			}), u.value = "", d.value = "", s.value = "Repository added. Save repositories to apply it.", c.value = "";
		}
		async function he() {
			o.value = "Saving Portal repositories…";
			try {
				await js(`${n.options.endpoints.shop}/repos`, { repos: re() }), L("Portal repositories saved."), await ae(!0);
			} catch (e) {
				L(e instanceof Error ? e.message : "Repository save failed.", "error");
			} finally {
				o.value = "";
			}
		}
		function ge(e) {
			e.key === "Escape" && de();
		}
		return On(() => n.state.payload, R, { deep: !1 }), R(), window.addEventListener("keydown", ge), kr(() => {
			v += 1, window.removeEventListener("keydown", ge);
		}), t({ select: a }), (e, t) => (q(), J("div", xv, [
			Y("header", Sv, [t[10] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Conversation surfaces"),
				Y("h1", null, "Portals"),
				Y("p", null, "Manage where Tater listens and responds, along with every Portal’s runtime and updates.")
			], -1), Y("div", Cv, [Y("span", { class: B(["tv-live-pill", { busy: !!o.value }]) }, [t[9] ||= Y("i", null, null, -1), X(V(o.value || "Live"), 1)], 2), Y("button", {
				class: "tv-button",
				type: "button",
				onClick: t[0] ||= (e) => ae()
			}, "Refresh")])]),
			Y("div", wv, [
				Y("div", null, [t[11] ||= Y("span", null, "Installed", -1), Y("strong", null, V(S.value.length || x.value.length), 1)]),
				Y("div", null, [t[12] ||= Y("span", null, "Running", -1), Y("strong", null, V(E.value), 1)]),
				Y("div", null, [t[13] ||= Y("span", null, "Store", -1), Y("strong", null, V(C.value.length), 1)]),
				Y("div", null, [t[14] ||= Y("span", null, "Updates", -1), Y("strong", null, V(Number(b.value.updates_available || T.value.length)), 1)])
			]),
			s.value || c.value ? (q(), J("div", {
				key: 0,
				class: B(["tv-notice", { error: !!c.value }])
			}, V(c.value || s.value), 3)) : Z("", !0),
			b.value.errors?.length ? (q(), J("div", Tv, V(b.value.errors.join(" • ")), 1)) : Z("", !0),
			Y("nav", Ev, [(q(), J(K, null, G(r, (e) => Y("button", {
				key: e.id,
				type: "button",
				class: B({ active: i.value === e.id }),
				onClick: (t) => a(e.id)
			}, [X(V(e.label), 1), e.id === "manage" && T.value.length ? (q(), J("span", Ov, V(T.value.length), 1)) : Z("", !0)], 10, Dv)), 64))]),
			i.value === "installed" ? (q(), J("section", kv, [(q(!0), J(K, null, G(A.value, (e) => (q(), J("article", {
				key: e.key,
				class: "tv-panel tp-portal-card"
			}, [
				Y("header", null, [Y("div", null, [Y("span", Av, V(e.key), 1), Y("h2", null, V(I(e)), 1)]), Y("span", { class: B(["tv-state", {
					good: e.runtime?.running,
					pending: e.runtime?.desired_running && !e.runtime?.running
				}]) }, V(ne(e.runtime)), 3)]),
				Y("p", null, V(ee(e)), 1),
				Y("div", jv, [
					Y("span", null, "Installed " + V(e.shop?.installed_ver || "0.0.0"), 1),
					Y("span", null, "Store " + V(e.shop?.store_ver || "-"), 1),
					Y("span", null, V(e.shop?.source_label || "local"), 1)
				]),
				Y("footer", null, [e.runtime?.settings?.length ? (q(), J("button", {
					key: 0,
					class: "tv-button",
					type: "button",
					onClick: (t) => fe(e.runtime)
				}, "Settings", 8, Mv)) : (q(), J("span", Nv, V(e.runtime ? "No configurable settings" : "Runtime unavailable"), 1)), e.runtime ? (q(), J("button", {
					key: 2,
					class: B(["tv-button", { primary: !e.runtime.running }]),
					type: "button",
					onClick: (t) => oe(e.runtime, e.runtime.running ? "stop" : "start")
				}, V(e.runtime.running ? "Stop" : "Start"), 11, Pv)) : Z("", !0)])
			]))), 128)), A.value.length ? Z("", !0) : (q(), J("div", Fv, "No installed Portals found."))])) : i.value === "store" ? (q(), J("section", Iv, [(q(!0), J(K, null, G(w.value, (e) => (q(), J("article", {
				key: e.id,
				class: "tv-panel tp-portal-card"
			}, [
				Y("header", null, [Y("div", null, [Y("span", Lv, V(e.id), 1), Y("h2", null, V(e.name || e.id), 1)]), Y("span", Rv, "v" + V(e.version || "-"), 1)]),
				Y("p", null, V(e.description || "No description provided."), 1),
				Y("footer", null, [Y("span", null, V(e.source_label || "Tater Shop"), 1), Y("button", {
					class: "tv-button primary",
					type: "button",
					onClick: (t) => se("install", e.id)
				}, "Install", 8, zv)])
			]))), 128)), w.value.length ? Z("", !0) : (q(), J("div", Bv, "No additional Portals are available from the configured repositories."))])) : i.value === "manage" ? (q(), J("section", Vv, [
				Y("div", Hv, [t[17] ||= Y("div", { class: "tv-manage-hero-copy" }, [
					Y("span", { class: "tv-eyebrow" }, "Manage library"),
					Y("h2", null, "Portal control center"),
					Y("p", null, "Update conversation surfaces, control their runtimes, and cleanly remove saved data. Running Portals restart automatically after an update.")
				], -1), Y("div", Uv, [
					Y("div", null, [t[15] ||= Y("span", null, "Installed", -1), Y("strong", null, V(S.value.length), 1)]),
					Y("div", { class: B({ attention: T.value.length }) }, [t[16] ||= Y("span", null, "Updates ready", -1), Y("strong", null, V(T.value.length), 1)], 2),
					Y("button", {
						class: "tv-button primary",
						type: "button",
						disabled: !T.value.length,
						onClick: t[1] ||= (e) => se("update-all")
					}, "Update all", 8, Wv)
				])]),
				(q(!0), J(K, null, G(S.value.slice().sort(F), (e) => (q(), J("article", {
					key: e.id,
					class: B(["tv-panel tp-manage-row tv-manage-card", { "has-update": e.update_available }])
				}, [
					Y("div", Gv, [Y("span", Kv, V(j(e.name || e.id).charAt(0).toUpperCase()), 1), Y("div", null, [
						Y("span", qv, V(e.id), 1),
						Y("h3", null, V(e.name || e.id), 1),
						Y("small", null, V(e.source_label || "Local Portal"), 1)
					])]),
					Y("div", Jv, [
						Y("div", null, [t[18] ||= Y("span", null, "Installed", -1), Y("strong", null, V(e.installed_ver || "0.0.0"), 1)]),
						t[20] ||= Y("i", null, "→", -1),
						Y("div", null, [t[19] ||= Y("span", null, "Latest", -1), Y("strong", null, V(e.store_ver || "-"), 1)]),
						Y("span", { class: B(["tv-state", e.update_available ? "pending" : "good"]) }, V(e.update_available ? "Update ready" : "Current"), 3)
					]),
					Y("div", Yv, [
						Y("span", { class: B(["tv-manage-runtime", { online: te(e)?.running }]) }, [t[21] ||= Y("i", null, null, -1), X(V(ne(te(e))), 1)], 2),
						Y("button", {
							class: B(["tv-button", { primary: e.update_available }]),
							type: "button",
							disabled: !e.update_available,
							onClick: (t) => se("update", e.id)
						}, V(e.update_available ? "Update" : "Current"), 11, Xv),
						te(e) ? (q(), J("button", {
							key: 0,
							class: "tv-button",
							type: "button",
							onClick: (t) => oe(te(e), te(e)?.running ? "stop" : "start")
						}, V(te(e)?.running ? "Stop" : "Start"), 9, Zv)) : Z("", !0),
						Y("label", Qv, [W(Y("input", {
							"onUpdate:modelValue": (t) => l.value[e.id] = t,
							type: "checkbox"
						}, null, 8, $v), [[cs, l.value[e.id]]]), t[22] ||= X(" Delete data", -1)]),
						Y("button", {
							class: "tv-button danger",
							type: "button",
							onClick: (t) => se("remove", e.id)
						}, "Remove", 8, ey)
					])
				], 2))), 128)),
				S.value.length ? Z("", !0) : (q(), J("div", ty, "No installed Portals found."))
			])) : (q(), J("section", ny, [
				t[35] ||= Y("header", { class: "tv-repository-heading" }, [Y("div", null, [
					Y("span", { class: "tv-eyebrow" }, "Repository library"),
					Y("h2", null, "Portal repositories"),
					Y("p", null, "Choose a Tater-trusted source or add your own manifest.")
				])], -1),
				Y("nav", ry, [Y("button", {
					type: "button",
					class: B({ active: p.value === "trusted" }),
					onClick: t[2] ||= (e) => p.value = "trusted"
				}, "Trusted repositories", 2), Y("button", {
					type: "button",
					class: B({ active: p.value === "custom" }),
					onClick: t[3] ||= (e) => p.value = "custom"
				}, "Custom repositories", 2)]),
				p.value === "trusted" ? (q(), J("div", iy, [
					t[31] ||= Y("p", { class: "tv-repository-intro" }, "Curated sources are reviewed by Tater. Select a card to add or remove its Portals from your Store automatically.", -1),
					b.value.repos?.trusted_error ? (q(), J("div", ay, "The trusted directory is temporarily unavailable. Your enabled repositories are unchanged.")) : Z("", !0),
					Y("div", oy, [Y("article", sy, [
						t[26] ||= Y("span", { class: "tv-repo-check" }, "✓", -1),
						Y("div", cy, [t[25] ||= Y("span", { class: "tv-repo-monogram" }, "T", -1), Y("div", null, [
							t[23] ||= Y("span", { class: "tv-eyebrow" }, "Always available", -1),
							Y("h3", null, V(b.value.repos?.default?.name || "Tater Portal Shop"), 1),
							t[24] ||= Y("p", null, [X("by "), Y("strong", null, "Tater Assistant")], -1)
						])]),
						t[27] ||= Y("p", null, "The official built-in Portal catalog.", -1),
						t[28] ||= Y("footer", null, [Y("span", { class: "tv-repo-enabled" }, "Built in")], -1)
					]), (q(!0), J(K, null, G(D.value, (e) => (q(), J("article", {
						key: e.id || e.url,
						class: B(["tv-trusted-repo-card", { selected: e.enabled }]),
						role: "button",
						tabindex: "0",
						"aria-pressed": !!e.enabled,
						onClick: (t) => ie(e),
						onKeydown: [Ss(bs((t) => ie(e), ["prevent"]), ["enter"]), Ss(bs((t) => ie(e), ["prevent"]), ["space"])]
					}, [
						Y("span", uy, V(e.enabled ? "✓" : "+"), 1),
						Y("div", dy, [Y("span", fy, V(j(e.repository || e.name).charAt(0).toUpperCase()), 1), Y("div", null, [
							t[30] ||= Y("span", { class: "tv-eyebrow" }, "Trusted Portal source", -1),
							Y("h3", null, V(e.repository || e.name), 1),
							Y("p", null, [t[29] ||= X("by ", -1), e.author_url ? (q(), J("a", {
								key: 0,
								href: e.author_url,
								target: "_blank",
								rel: "noreferrer",
								onClick: t[4] ||= bs(() => {}, ["stop"])
							}, V(e.author || "Community author"), 9, py)) : (q(), J("strong", my, V(e.author || "Community author"), 1))])
						])]),
						Y("p", null, V(e.description || "Additional Portals for the Tater Store."), 1),
						e.tags?.length ? (q(), J("div", hy, [(q(!0), J(K, null, G(e.tags, (e) => (q(), J("span", { key: e }, V(e), 1))), 128))])) : Z("", !0),
						Y("footer", null, [e.homepage ? (q(), J("a", {
							key: 0,
							href: e.homepage,
							target: "_blank",
							rel: "noreferrer",
							onClick: t[5] ||= bs(() => {}, ["stop"])
						}, "View repository ↗", 8, gy)) : Z("", !0), Y("span", { class: B(e.enabled ? "tv-repo-enabled" : "tv-repo-available") }, V(e.enabled ? "Added to Store" : "Select to add"), 3)])
					], 42, ly))), 128))]),
					D.value.length ? Z("", !0) : (q(), J("div", _y, "No additional trusted Portal repositories are listed yet."))
				])) : (q(), J("div", vy, [
					t[34] ||= Y("p", { class: "tv-repository-intro" }, "Custom manifests are managed by you and are not reviewed by Tater.", -1),
					(q(!0), J(K, null, G(f.value, (e, t) => (q(), J("article", {
						key: `${e.url}-${t}`,
						class: "ti-repo-row"
					}, [Y("div", null, [Y("strong", null, V(e.name || "Custom repository"), 1), Y("code", null, V(e.url), 1)]), Y("button", {
						class: "tv-button",
						type: "button",
						onClick: (e) => f.value.splice(t, 1)
					}, "Remove", 8, yy)]))), 128)),
					f.value.length ? Z("", !0) : (q(), J("div", by, "No custom repositories configured.")),
					Y("div", xy, [
						Y("label", null, [t[32] ||= Y("span", null, "Name (optional)", -1), W(Y("input", {
							"onUpdate:modelValue": t[6] ||= (e) => u.value = e,
							type: "text",
							placeholder: "My Portal Repo"
						}, null, 512), [[$, u.value]])]),
						Y("label", null, [t[33] ||= Y("span", null, "Manifest URL", -1), W(Y("input", {
							"onUpdate:modelValue": t[7] ||= (e) => d.value = e,
							type: "url",
							placeholder: "https://example.com/portals.json",
							onKeyup: Ss(me, ["enter"])
						}, null, 544), [[$, d.value]])]),
						Y("button", {
							class: "tv-button",
							type: "button",
							onClick: me
						}, "Add"),
						Y("button", {
							class: "tv-button primary",
							type: "button",
							onClick: he
						}, "Save custom repositories")
					])
				]))
			])),
			ga(kl, {
				open: !!m.value,
				onClose: de
			}, {
				default: Sn(() => [Y("form", {
					class: "tv-modal tp-settings-modal",
					onSubmit: bs(pe, ["prevent"])
				}, [
					Y("header", null, [Y("div", null, [Y("span", Sy, V(m.value?.key), 1), Y("h2", null, V(m.value?.label || m.value?.key) + " settings", 1)]), Y("button", {
						class: "tv-button",
						type: "button",
						onClick: de
					}, "Close")]),
					_.value ? (q(), J("div", Cy, V(_.value), 1)) : Z("", !0),
					g.value ? (q(), J("div", wy, "Loading live Portal settings…")) : (q(), J("div", Ty, [(q(!0), J(K, null, G(m.value?.settings || [], (e, n) => (q(), da(zd, {
						key: e.key || n,
						modelValue: h.value[e.key],
						"onUpdate:modelValue": (t) => h.value[e.key] = t,
						field: e,
						"all-values": h.value,
						onError: t[8] ||= (e) => L(e, "error"),
						onNotify: L
					}, null, 8, [
						"modelValue",
						"onUpdate:modelValue",
						"field",
						"all-values"
					]))), 128))])),
					Y("footer", null, [Y("span", null, V(g.value ? "Loading live options…" : o.value || s.value), 1), Y("button", {
						class: "tv-button primary",
						type: "submit",
						disabled: g.value
					}, "Save settings", 8, Ey)])
				], 32)]),
				_: 1
			}, 8, ["open"])
		]));
	}
}), Oy = { class: "tater-vue-surface tsx-spudex" }, ky = { class: "tsx-spudex-header" }, Ay = {
	class: "tv-tabs tsx-tabs",
	"aria-label": "Spudex sections"
}, jy = ["onClick"], My = { key: 0 }, Ny = { class: "tsx-header-actions" }, Py = {
	key: 1,
	class: "tsx-workbench"
}, Fy = { class: "tsx-spud-bar" }, Iy = { class: "tsx-session-switcher" }, Ly = { class: "tsx-bar-label" }, Ry = ["value"], zy = {
	key: 0,
	value: ""
}, By = ["value"], Vy = ["disabled"], Hy = { class: "tsx-runtime-strip" }, Uy = { class: "tsx-bar-label" }, Wy = { class: "tsx-process-strip" }, Gy = {
	key: 0,
	class: "tsx-runtime-quiet"
}, Ky = ["title"], qy = ["onClick"], Jy = { class: "tsx-session-tools" }, Yy = ["disabled"], Xy = ["disabled"], Zy = ["disabled"], Qy = { class: "tsx-workbench-grid" }, $y = { class: "tv-panel tsx-chat-card" }, eb = { class: "tsx-pane-head" }, tb = { class: "tsx-chat-scroll" }, nb = {
	key: 0,
	class: "tsx-chat-feed"
}, rb = {
	key: 1,
	class: "tsx-chat-empty"
}, ib = ["disabled"], ab = ["disabled"], ob = { class: "tv-panel tsx-terminal-card" }, sb = { class: "tsx-console-head" }, cb = { class: "tsx-terminal-actions" }, lb = ["disabled"], ub = {
	class: "tsx-terminal-body",
	role: "log",
	"aria-label": "Spudex command output",
	"aria-live": "polite"
}, db = {
	key: 0,
	class: "tsx-log-list"
}, fb = {
	key: 1,
	class: "tsx-terminal-empty"
}, pb = { class: "tsx-terminal-status" }, mb = {
	key: 2,
	class: "tsx-manual"
}, hb = { class: "tsx-manual-terminal" }, gb = { class: "tsx-manual-head" }, _b = { class: "tsx-manual-actions" }, vb = { class: "tsx-manual-state" }, yb = ["disabled"], bb = ["disabled"], xb = ["disabled"], Sb = {
	class: "tsx-manual-console-body",
	role: "log",
	"aria-label": "Manual terminal output",
	"aria-live": "polite"
}, Cb = {
	key: 0,
	class: "tsx-manual-welcome"
}, wb = { class: "tsx-prompt-line" }, Tb = ["disabled"], Eb = { class: "tsx-terminal-check" }, Db = ["disabled"], Ob = { class: "tsx-manual-footer" }, kb = {
	key: 3,
	class: "tsx-settings"
}, Ab = { class: "tv-panel tsx-access-card" }, jb = { class: "tsx-master-toggle" }, Mb = { class: "tsx-settings-grid" }, Nb = { class: "tsx-platforms" }, Pb = ["checked", "onChange"], Fb = { class: "tv-panel tsx-policy-card" }, Ib = {
	key: 0,
	class: "tsx-policy-notice danger"
}, Lb = { class: "tsx-settings-save" }, Rb = ["disabled"], zb = {
	class: "tv-modal tsx-details",
	role: "dialog",
	"aria-modal": "true",
	"aria-label": "Session details"
}, Bb = {
	key: 0,
	class: "tsx-insights"
}, Vb = {
	key: 0,
	class: "tsx-policy-notice danger"
}, Hb = { key: 0 }, Ub = {
	key: 0,
	class: "tsx-plan"
}, Wb = {
	key: 1,
	class: "tv-empty compact"
}, Gb = { key: 0 }, Kb = {
	key: 1,
	class: "tv-empty compact"
}, qb = {
	key: 0,
	class: "tsx-preview-list"
}, Jb = ["href"], Yb = {
	key: 1,
	class: "tv-empty compact"
}, Xb = {
	key: 0,
	class: "tsx-git"
}, Zb = { key: 0 }, Qb = {
	key: 1,
	class: "tv-empty compact"
}, $b = { class: "wide" }, ex = {
	key: 0,
	class: "tsx-file-list"
}, tx = { key: 0 }, nx = ["onClick"], rx = ["onClick"], ix = {
	key: 1,
	class: "tv-empty compact"
}, ax = { class: "wide" }, ox = { key: 0 }, sx = {
	key: 1,
	class: "tv-empty compact"
}, cx = {
	key: 1,
	class: "tv-empty"
}, lx = /* @__PURE__ */ sr({
	__name: "SpudexApp",
	props: {
		state: {},
		options: {}
	},
	setup(e, { expose: t }) {
		let n = e, r = [
			{
				id: "workbench",
				label: "Workbench"
			},
			{
				id: "manual",
				label: "Manual Session"
			},
			{
				id: "settings",
				label: "Settings"
			}
		], i = /* @__PURE__ */ U(ie(n.options.initialTab)), a = /* @__PURE__ */ U(R(n.options.initialSessionId)), o = /* @__PURE__ */ U(R(n.options.initialManualSessionId)), s = /* @__PURE__ */ U([]), c = /* @__PURE__ */ U(0), l = /* @__PURE__ */ U([]), u = /* @__PURE__ */ U(0), d = /* @__PURE__ */ U(""), f = /* @__PURE__ */ U(""), p = /* @__PURE__ */ U("agent_lab"), m = /* @__PURE__ */ U("~"), h = /* @__PURE__ */ U(!1), g = /* @__PURE__ */ U(""), _ = /* @__PURE__ */ U(""), v = /* @__PURE__ */ U(""), y = /* @__PURE__ */ U(!1), b = /* @__PURE__ */ U(!1), x = /* @__PURE__ */ Et({}), S = 0, C = !1, w = Q(() => n.state.payload || {}), T = Q(() => Array.isArray(w.value.sessions) ? w.value.sessions : []), E = Q(() => T.value.filter((e) => z(e.source) === "ui")), D = Q(() => Array.isArray(w.value.model_processes) ? w.value.model_processes : []), O = Q(() => T.value.find((e) => R(e.id) === a.value) || null), k = Q(() => E.value.find((e) => R(e.id) === o.value) || null), A = Q(() => i.value === "manual" ? k.value : O.value), j = Q(() => Number(w.value.active_count || T.value.filter(ae).length)), M = Q(() => Number(w.value.model_process_count || D.value.length)), N = Q(() => ae(O.value)), P = Q(() => ae(k.value)), F = Q(() => z(O.value?.source) === "spudex_chat" ? O.value : null), I = Q(() => !!(g.value === "chat" || ae(F.value))), ee = Q(() => le(w.value.platform_options, x.allowed_platforms)), te = Q(() => {
			let e = s.value.map((e) => {
				let t = z(e.stream), r = R(e.text);
				return r ? t === "user" ? {
					role: "user",
					username: n.options.profile?.username,
					content: r
				} : t === "assistant" ? {
					role: "assistant",
					content: r
				} : null : null;
			}).filter(Boolean);
			return I.value ? [...e.slice(-20), {
				role: "assistant",
				content: { marker: "typing" }
			}] : e.slice(-20);
		}), ne = Q(() => s.value.filter((e) => !["user", "assistant"].includes(z(e.stream)))), L = Q(() => {
			let e = F.value || O.value;
			if (g.value === "chat") return "Starting Spudex chat…";
			if (!e) return j.value ? `${j.value} active Spudex process${j.value === 1 ? "" : "es"}` : "Ready for a Spudex task.";
			let t = (Array.isArray(e.plan) ? e.plan : []).find((e) => z(e.status) === "in_progress");
			return t?.step ? `Working: ${t.step}` : `${oe(e.status)}${R(e.label || e.command || e.goal) ? `: ${R(e.label || e.command || e.goal)}` : ""}`;
		});
		function R(e) {
			return String(e ?? "").trim();
		}
		function z(e) {
			return R(e).toLowerCase();
		}
		function re(e) {
			return encodeURIComponent(R(e));
		}
		function ie(e) {
			let t = z(e);
			return t === "manual" || t === "settings" || t === "policy" ? t === "policy" ? "settings" : t : "workbench";
		}
		function ae(e) {
			let t = z(e?.status);
			return !!e?.active || t === "running" || t === "queued";
		}
		function oe(e) {
			let t = z(e) || "queued";
			return {
				succeeded: "Done",
				completed: "Complete",
				failed: "Failed",
				running: "Running",
				blocked: "Blocked",
				timeout: "Timeout",
				stopped: "Stopped",
				incomplete: "Incomplete",
				queued: "Queued",
				draft: "Draft"
			}[t] || t.replaceAll("_", " ").replace(/^./, (e) => e.toUpperCase());
		}
		function se(e) {
			let t = Number(e || 0);
			if (!t) return "";
			let n = Math.max(0, Math.floor(Date.now() / 1e3 - t));
			return n < 60 ? `${n}s ago` : n < 3600 ? `${Math.floor(n / 60)}m ago` : n < 86400 ? `${Math.floor(n / 3600)}h ago` : `${Math.floor(n / 86400)}d ago`;
		}
		function ce(e, t, n) {
			let r = n ? [] : [...e], i = (e) => `${e._session_id ?? ""}\u0000${R(e.seq) || `${e.ts ?? ""}\u0000${e.stream ?? ""}\u0000${e.text ?? ""}`}`, a = new Set(r.map(i));
			return t.forEach((e) => {
				let t = i(e);
				a.has(t) || (a.add(t), r.push(e));
			}), r.slice(-1e3);
		}
		function le(e, t) {
			let n = new Set((Array.isArray(t) ? t : ["webui"]).map(z).filter(Boolean)), r = /* @__PURE__ */ new Map();
			return (Array.isArray(e) ? e : []).forEach((e) => {
				let t = z(e.value);
				t && !r.has(t) && r.set(t, {
					...e,
					value: t
				});
			}), n.forEach((e) => {
				r.has(e) || r.set(e, {
					value: e,
					label: e === "all" ? "All platforms" : e.replaceAll("_", " "),
					description: "Saved platform, currently stopped",
					running: e === "all"
				});
			}), r.size || r.set("webui", {
				value: "webui",
				label: "Web UI",
				description: "Tater browser UI",
				running: !0
			}), [...r.values()];
		}
		function ue(e, t = "success") {
			v.value = e, _.value = t === "error" ? e : "", n.options.onToast?.(e, t);
		}
		function de(e, t = "") {
			return `${n.options.endpoints.sessions}/${re(e)}${t}`;
		}
		async function fe(e) {
			return ks(await fetch(e, {
				method: "DELETE",
				credentials: "same-origin",
				headers: { Accept: "application/json" }
			}));
		}
		function pe(e = !1) {
			if (b.value && !e) return;
			let t = w.value.settings || {};
			Object.assign(x, {
				enabled: !!t.enabled,
				full_access: !!t.full_access,
				allowed_platforms: Array.isArray(t.allowed_platforms) ? [...t.allowed_platforms] : ["webui"],
				default_cwd: R(t.default_cwd || "agent_lab"),
				max_task_steps: Number(t.max_task_steps || 6),
				command_timeout_sec: Number(t.command_timeout_sec || 45)
			}), b.value = !1;
		}
		function me() {
			T.value.some((e) => R(e.id) === a.value) || ge(R(T.value[0]?.id), !1), E.value.some((e) => R(e.id) === o.value) || _e(R(E.value[0]?.id), !1);
		}
		function he(e) {
			i.value = ie(e), y.value = !1, n.options.onTabChange?.(i.value);
		}
		function ge(e, t = !0) {
			let r = R(e);
			r !== a.value && (a.value = r, s.value = [], c.value = 0, n.options.onSessionChange?.(r), t && be(!0));
		}
		function _e(e, t = !0, r = !1) {
			let i = R(e);
			i !== o.value && (o.value = i, r || (l.value = []), u.value = 0, n.options.onManualSessionChange?.(i), i && (a.value = i, n.options.onSessionChange?.(i)), t && xe(!r));
		}
		async function ve(e = !1) {
			e || (g.value = "refresh");
			try {
				n.state.payload = await As(n.options.endpoints.root), me(), pe();
			} catch (t) {
				e || ue(t instanceof Error ? t.message : "Spudex refresh failed.", "error");
			} finally {
				!e && g.value === "refresh" && (g.value = "");
			}
		}
		async function ye(e, t) {
			return As(`${de(e, "/logs")}?after_seq=${re(t)}&limit=500`);
		}
		async function be(e = !1) {
			let t = a.value;
			if (!t) {
				s.value = [], c.value = 0;
				return;
			}
			let n = await ye(t, e ? 0 : c.value), r = Array.isArray(n.entries) ? n.entries : [];
			s.value = ce(s.value, r, e), c.value = Number(n.last_seq || (e ? 0 : c.value)), await un(), document.querySelectorAll(".tsx-chat-scroll, .tsx-terminal-body").forEach((t) => {
				t instanceof HTMLElement && (e || t.scrollHeight - t.scrollTop - t.clientHeight < 120) && (t.scrollTop = t.scrollHeight);
			});
		}
		async function xe(e = !1) {
			let t = o.value;
			if (!t) {
				l.value = [], u.value = 0;
				return;
			}
			let n = await ye(t, e ? 0 : u.value), r = (Array.isArray(n.entries) ? n.entries : []).map((e) => ({
				...e,
				_session_id: t
			}));
			l.value = ce(l.value, r, e), u.value = Number(n.last_seq || (e ? 0 : u.value)), await un();
			let i = document.querySelector(".tsx-manual-console-body");
			i instanceof HTMLElement && (e || i.scrollHeight - i.scrollTop - i.clientHeight < 100) && (i.scrollTop = i.scrollHeight);
		}
		async function H(e = !1) {
			await ve(e), await Promise.all([be(!1), xe(!1)]);
		}
		function Se() {
			S && window.clearTimeout(S), S = window.setTimeout(async () => {
				if (!C) {
					C = !0;
					try {
						await H(!0);
					} catch {} finally {
						C = !1;
					}
				}
				Se();
			}, 2e3);
		}
		async function Ce() {
			let e = d.value.trim();
			if (!e) {
				ue("Enter a Spudex chat message first.", "error");
				return;
			}
			if (I.value) {
				ue("Spudex is still working in this chat.", "error");
				return;
			}
			g.value = "chat";
			try {
				let t = z(O.value?.source) === "spudex_chat" ? a.value : "", r = R((await js(n.options.endpoints.chat, {
					message: e,
					session_id: t || null
				})).session?.id);
				r && ge(r, !1), d.value = "", ue("Spudex task started."), await ve(!0), await be(!0);
			} catch (e) {
				ue(e instanceof Error ? e.message : "Spudex chat failed.", "error");
			} finally {
				g.value = "";
			}
		}
		async function we() {
			g.value = "new-chat";
			try {
				ge(R((await js(n.options.endpoints.chatSession, { label: "New Spudex chat" })).session?.id), !1), d.value = "", ue("New Spudex chat created."), await ve(!0), await be(!0);
			} catch (e) {
				ue(e instanceof Error ? e.message : "New Spudex chat failed.", "error");
			} finally {
				g.value = "";
			}
		}
		async function Te() {
			let e = f.value.trim();
			if (!e) {
				ue("Enter a command first.", "error");
				return;
			}
			g.value = "run";
			try {
				let t = await js(n.options.endpoints.run, {
					command: e,
					cwd: p.value,
					label: e.slice(0, 80),
					background: h.value
				}), r = R(t.session?.id);
				p.value = R(t.session?.cwd) || p.value, m.value = R(t.session?.cwd_display) || m.value, ge(r, !1), _e(r, !1, !0), f.value = "", ue(t.builtin === "cd" ? `Working directory: ${m.value}` : t.builtin ? "Command completed." : "Spudex session started."), await ve(!0), await Promise.all([be(!0), xe(!1)]);
			} catch (e) {
				ue(e instanceof Error ? e.message : "Command failed.", "error");
			} finally {
				g.value = "";
			}
		}
		async function Ee(e, t = "Spudex session") {
			if (e) {
				g.value = `stop-${e}`;
				try {
					await js(de(e, "/stop")), ue(`${t} stop requested.`), await ve(!0);
				} catch (e) {
					ue(e instanceof Error ? e.message : "Stop failed.", "error");
				} finally {
					g.value = "";
				}
			}
		}
		async function De(e) {
			let t = R(e.id);
			if (t && !(ae(e) && !window.confirm("Close this running Spudex session? Its active command will be stopped."))) {
				g.value = `close-${t}`;
				try {
					await fe(de(t)), t === a.value && ge("", !1), t === o.value && _e("", !1), ue("Spudex session closed."), await ve(!0);
				} catch (e) {
					ue(e instanceof Error ? e.message : "Close failed.", "error");
				} finally {
					g.value = "";
				}
			}
		}
		async function Oe(e, t, n) {
			g.value = `${n}-${t}`;
			try {
				await js(de(e, `/file-changes/${n}`), { change_id: t }), ue(`File change ${n === "approve" ? "approved" : "rejected"}.`), await ve(!0);
			} catch (e) {
				ue(e instanceof Error ? e.message : "File change update failed.", "error");
			} finally {
				g.value = "";
			}
		}
		async function ke() {
			g.value = "settings";
			try {
				await js(n.options.endpoints.settings, { values: {
					...x,
					allowed_platforms: x.allowed_platforms?.length ? x.allowed_platforms : ["webui"]
				} }), b.value = !1, ue("Spudex settings saved."), await ve(!0), pe(!0);
			} catch (e) {
				ue(e instanceof Error ? e.message : "Spudex settings failed.", "error");
			} finally {
				g.value = "";
			}
		}
		function Ae(e, t) {
			let n = new Set((Array.isArray(x.allowed_platforms) ? x.allowed_platforms : []).map(z));
			t ? (e === "all" && n.clear(), n.add(e)) : n.delete(e), e !== "all" && t && n.delete("all"), x.allowed_platforms = [...n], b.value = !0;
		}
		function je(e) {
			e.key === "Enter" && !e.shiftKey && !e.ctrlKey && !e.altKey && !e.metaKey && !e.isComposing && (e.preventDefault(), Ce());
		}
		function Me(e) {
			ge(e.target.value);
		}
		function Ne() {
			s.value = s.value.filter((e) => ["user", "assistant"].includes(z(e.stream)));
		}
		function Pe() {
			O.value && De(O.value);
		}
		function Fe(e) {
			e.key === "Escape" && (y.value = !1);
		}
		return On(() => n.state.payload, () => {
			me(), pe();
		}, { deep: !1 }), pe(!0), me(), window.addEventListener("keydown", Fe), Promise.all([be(!0), xe(!0)]).catch(() => {}), Se(), kr(() => {
			S && window.clearTimeout(S), window.removeEventListener("keydown", Fe);
		}), t({ refresh: () => H(!1) }), (t, n) => (q(), J(K, null, [Y("div", Oy, [
			Y("header", ky, [
				n[22] ||= Y("div", { class: "tsx-brand" }, [Y("span", {
					class: "tsx-spud-mark",
					"aria-hidden": "true"
				}, [
					Y("i"),
					Y("i"),
					Y("i")
				]), Y("div", null, [Y("span", { class: "tv-eyebrow" }, "Tater agent workspace"), Y("h1", null, "Spudex")])], -1),
				Y("nav", Ay, [(q(), J(K, null, G(r, (e) => Y("button", {
					key: e.id,
					type: "button",
					class: B({ active: i.value === e.id }),
					onClick: (t) => he(e.id)
				}, [X(V(e.label), 1), e.id === "workbench" && j.value ? (q(), J("span", My, V(j.value), 1)) : Z("", !0)], 10, jy)), 64))]),
				Y("div", Ny, [Y("span", { class: B(["tv-live-pill", { busy: !!g.value }]) }, [n[21] ||= Y("i", null, null, -1), X(V(g.value ? "Working" : "Live"), 1)], 2), Y("button", {
					class: "tv-button tsx-icon-button",
					type: "button",
					"aria-label": "Refresh Spudex",
					title: "Refresh",
					onClick: n[0] ||= (e) => H(!1)
				}, "↻")])
			]),
			v.value || _.value ? (q(), J("div", {
				key: 0,
				class: B(["tv-notice", { error: !!_.value }])
			}, V(_.value || v.value), 3)) : Z("", !0),
			i.value === "workbench" ? (q(), J("section", Py, [Y("div", Fy, [
				Y("div", Iy, [
					Y("span", Ly, [Y("i", { class: B({ live: N.value }) }, null, 2), n[23] ||= X("Session", -1)]),
					Y("select", {
						value: a.value,
						"aria-label": "Selected Spudex session",
						onChange: Me
					}, [T.value.length ? Z("", !0) : (q(), J("option", zy, "No sessions yet")), (q(!0), J(K, null, G(T.value, (e) => (q(), J("option", {
						key: e.id,
						value: String(e.id)
					}, V(e.label || e.command || "Spudex session") + " · " + V(oe(e.status)), 9, By))), 128))], 40, Ry),
					Y("button", {
						class: "tv-button primary tsx-new-chat",
						type: "button",
						disabled: g.value === "new-chat",
						onClick: we
					}, [...n[24] ||= [Y("span", { "aria-hidden": "true" }, "＋", -1), X(" New chat", -1)]], 8, Vy)
				]),
				Y("div", Hy, [Y("span", Uy, [
					Y("i", { class: B({ live: D.value.length }) }, null, 2),
					n[25] ||= X("Runtime ", -1),
					Y("b", null, V(M.value), 1)
				]), Y("div", Wy, [D.value.length ? Z("", !0) : (q(), J("span", Gy, "No tracked processes")), (q(!0), J(K, null, G(D.value, (e) => (q(), J("article", {
					key: e.session_id,
					title: [e.command, e.cwd].filter(Boolean).join(" · ")
				}, [
					Y("span", null, V(e.label || e.command || "Spudex process"), 1),
					Y("small", null, V(e.pid ? `PID ${e.pid}` : "Starting"), 1),
					Y("button", {
						type: "button",
						"aria-label": "Stop model process",
						title: "Kill process",
						onClick: (t) => Ee(String(e.session_id), "Model process")
					}, "×", 8, qy)
				], 8, Ky))), 128))])]),
				Y("div", Jy, [
					Y("button", {
						class: "tv-button",
						type: "button",
						disabled: !O.value,
						onClick: n[1] ||= (e) => y.value = !0
					}, "Details", 8, Yy),
					Y("button", {
						class: "tv-button danger",
						type: "button",
						disabled: !N.value,
						onClick: n[2] ||= (e) => Ee(a.value)
					}, "Stop", 8, Xy),
					Y("button", {
						class: "tv-button tsx-icon-button",
						type: "button",
						disabled: !O.value,
						"aria-label": "Close selected session",
						title: "Close session",
						onClick: Pe
					}, "×", 8, Zy)
				])
			]), Y("div", Qy, [Y("section", $y, [
				Y("header", eb, [Y("div", null, [n[27] ||= Y("span", {
					class: "tsx-pane-icon chat",
					"aria-hidden": "true"
				}, "✦", -1), Y("div", null, [n[26] ||= Y("strong", null, "Chat with Tater", -1), Y("small", null, V(O.value?.label || O.value?.command || "A fresh Spudex chat"), 1)])]), Y("span", { class: B(["tv-state", { good: N.value }]) }, V(O.value ? oe(O.value.status) : "Ready"), 3)]),
				Y("div", tb, [F.value && te.value.length ? (q(), J("div", nb, [(q(!0), J(K, null, G(te.value, (t, n) => (q(), da(sc, {
					key: `${n}-${t.role}`,
					message: t,
					profile: e.options.profile || {},
					"files-endpoint": e.options.endpoints.chatFiles
				}, null, 8, [
					"message",
					"profile",
					"files-endpoint"
				]))), 128))])) : I.value ? Z("", !0) : (q(), J("div", rb, [...n[28] ||= [
					Y("span", {
						class: "tsx-spud-mark large",
						"aria-hidden": "true"
					}, [
						Y("i"),
						Y("i"),
						Y("i")
					], -1),
					Y("h2", null, "What are we building?", -1),
					Y("p", null, "Ask Tater to inspect, run, or fix something through Spudex.", -1)
				]]))]),
				Y("form", {
					class: "tsx-composer",
					onSubmit: bs(Ce, ["prevent"])
				}, [
					W(Y("textarea", {
						"onUpdate:modelValue": n[3] ||= (e) => d.value = e,
						rows: "1",
						placeholder: "Message Tater through Spudex…",
						disabled: I.value,
						onKeydown: je
					}, null, 40, ib), [[$, d.value]]),
					Y("button", {
						class: "tv-button primary",
						type: "submit",
						disabled: I.value || !d.value.trim()
					}, V(I.value ? "Working…" : "Send"), 9, ab),
					Y("small", null, V(L.value), 1)
				], 32)
			]), Y("section", ob, [
				Y("header", sb, [Y("div", null, [n[30] ||= Y("span", {
					class: "tsx-window-dots",
					"aria-hidden": "true"
				}, [
					Y("i"),
					Y("i"),
					Y("i")
				], -1), Y("div", null, [n[29] ||= Y("strong", null, "Activity terminal", -1), Y("small", null, "tater@spudex:" + V(O.value?.cwd_display || "~"), 1)])]), Y("div", cb, [n[31] ||= Y("span", null, "Read only", -1), Y("button", {
					class: "tv-button",
					type: "button",
					disabled: !ne.value.length,
					onClick: Ne
				}, "Clear", 8, lb)])]),
				Y("div", ub, [ne.value.length ? (q(), J("div", db, [(q(!0), J(K, null, G(ne.value, (e) => (q(), J("article", {
					key: e.seq || `${e.ts}-${e.text}`,
					class: B(z(e.stream))
				}, [
					Y("time", null, V(se(e.ts)), 1),
					Y("span", null, V(e.stream === "command" ? "$" : e.stream || "log"), 1),
					Y("pre", null, V(String(e.text || "").replace(/^\$\s*/, "")), 1)
				], 2))), 128))])) : (q(), J("div", fb, [...n[32] ||= [
					Y("span", null, ">_", -1),
					Y("strong", null, "Waiting for activity", -1),
					Y("small", null, "Commands, tool output, and system messages will appear here.", -1)
				]]))]),
				Y("footer", pb, [Y("span", null, [Y("i", { class: B({ live: N.value }) }, null, 2), X(V(N.value ? "Session active" : "Standing by"), 1)]), Y("span", null, V(O.value ? `#${String(O.value.id).slice(0, 8)}` : "No session"), 1)])
			])])])) : i.value === "manual" ? (q(), J("section", mb, [Y("section", hb, [
				Y("header", gb, [Y("div", null, [n[34] ||= Y("span", {
					class: "tsx-window-dots",
					"aria-hidden": "true"
				}, [
					Y("i"),
					Y("i"),
					Y("i")
				], -1), Y("div", null, [n[33] ||= Y("strong", null, "Spudex Terminal", -1), Y("small", null, "tater@spudex:" + V(m.value), 1)])]), Y("div", _b, [
					Y("span", vb, [Y("i", { class: B({ live: P.value || g.value === "run" }) }, null, 2), X(V(g.value === "run" || P.value ? "Running" : k.value ? oe(k.value.status) : "Ready"), 1)]),
					Y("button", {
						class: "tv-button",
						type: "button",
						disabled: !k.value,
						onClick: n[4] ||= (e) => y.value = !0
					}, "Details", 8, yb),
					Y("button", {
						class: "tv-button",
						type: "button",
						disabled: !l.value.length,
						onClick: n[5] ||= (e) => l.value = []
					}, "Clear", 8, bb),
					Y("button", {
						class: "tv-button danger",
						type: "button",
						disabled: !P.value,
						onClick: n[6] ||= (e) => Ee(o.value, "Manual session")
					}, "Stop", 8, xb)
				])]),
				Y("div", Sb, [(q(!0), J(K, null, G(l.value, (e) => (q(), J("article", {
					key: `${e._session_id || ""}-${e.seq || e.ts || ""}-${e.text || ""}`,
					class: B(z(e.stream))
				}, [Y("span", null, V(e.stream === "command" ? "$" : e.stream || "log"), 1), Y("pre", null, V(String(e.text || "").replace(/^\$\s*/, "")), 1)], 2))), 128)), l.value.length ? Z("", !0) : (q(), J("div", Cb, [
					n[35] ||= Y("span", { class: "tsx-terminal-glyph" }, ">_", -1),
					n[36] ||= Y("strong", null, "Manual terminal ready.", -1),
					Y("small", null, "Starts in Agent Lab. " + V(x.full_access ? "Full access is on, so commands run through the host shell." : "Restricted mode is on."), 1)
				]))]),
				Y("form", {
					class: "tsx-manual-prompt",
					onSubmit: bs(Te, ["prevent"])
				}, [
					Y("label", wb, [n[37] ||= Y("span", { "aria-hidden": "true" }, "$", -1), W(Y("input", {
						"onUpdate:modelValue": n[7] ||= (e) => f.value = e,
						type: "text",
						autocomplete: "off",
						"aria-label": "Terminal command",
						placeholder: "Type a command…",
						disabled: g.value === "run"
					}, null, 8, Tb), [[$, f.value]])]),
					Y("label", Eb, [W(Y("input", {
						"onUpdate:modelValue": n[8] ||= (e) => h.value = e,
						class: "tv-checkbox",
						type: "checkbox"
					}, null, 512), [[cs, h.value]]), n[38] ||= Y("span", null, "Keep running", -1)]),
					Y("button", {
						class: "tv-button primary tsx-terminal-run",
						type: "submit",
						disabled: g.value === "run" || !f.value.trim()
					}, [n[39] ||= Y("span", { "aria-hidden": "true" }, "↵", -1), X(V(g.value === "run" ? "Running…" : "Run"), 1)], 8, Db)
				], 32),
				Y("footer", Ob, [Y("span", null, V(x.full_access ? "Full host access" : "Restricted mode"), 1), Y("span", null, "Current directory " + V(m.value), 1)])
			])])) : (q(), J("section", kb, [
				Y("div", Ab, [
					Y("header", null, [n[40] ||= Y("div", null, [
						Y("span", { class: "tv-eyebrow" }, "Hydra access"),
						Y("h2", null, "Spudex availability"),
						Y("p", null, "Choose where Hydra can expose Spudex terminal tools.")
					], -1), Y("label", jb, [Y("span", null, V(x.enabled ? "Enabled" : "Off"), 1), W(Y("input", {
						"onUpdate:modelValue": n[9] ||= (e) => x.enabled = e,
						class: "tv-checkbox",
						type: "checkbox",
						onChange: n[10] ||= (e) => b.value = !0
					}, null, 544), [[cs, x.enabled]])])]),
					Y("div", Mb, [
						Y("label", null, [n[41] ||= Y("span", null, "Starting folder", -1), W(Y("input", {
							"onUpdate:modelValue": n[11] ||= (e) => x.default_cwd = e,
							type: "text",
							onInput: n[12] ||= (e) => b.value = !0
						}, null, 544), [[$, x.default_cwd]])]),
						Y("label", null, [n[42] ||= Y("span", null, "Max task steps", -1), W(Y("input", {
							"onUpdate:modelValue": n[13] ||= (e) => x.max_task_steps = e,
							type: "number",
							min: "1",
							max: "50",
							onInput: n[14] ||= (e) => b.value = !0
						}, null, 544), [[
							$,
							x.max_task_steps,
							void 0,
							{ number: !0 }
						]])]),
						Y("label", null, [n[43] ||= Y("span", null, "Command timeout (seconds)", -1), W(Y("input", {
							"onUpdate:modelValue": n[15] ||= (e) => x.command_timeout_sec = e,
							type: "number",
							min: "5",
							max: "3600",
							onInput: n[16] ||= (e) => b.value = !0
						}, null, 544), [[
							$,
							x.command_timeout_sec,
							void 0,
							{ number: !0 }
						]])])
					]),
					Y("div", Nb, [n[44] ||= Y("div", null, [Y("strong", null, "Platforms"), Y("small", null, "Select where Hydra can expose Spudex.")], -1), (q(!0), J(K, null, G(ee.value, (e) => (q(), J("label", {
						key: e.value,
						class: B({ running: e.running })
					}, [Y("span", null, [Y("strong", null, V(e.label || e.value), 1), Y("small", null, V(e.value === "all" ? "Every platform" : e.running ? "Running" : "Stopped") + " · " + V(e.description || "Available platform"), 1)]), Y("input", {
						class: "tv-checkbox",
						type: "checkbox",
						checked: x.allowed_platforms?.includes(e.value),
						onChange: (t) => Ae(String(e.value), t.target.checked)
					}, null, 40, Pb)], 2))), 128))])
				]),
				Y("div", Fb, [
					Y("header", null, [n[45] ||= Y("div", null, [
						Y("span", { class: "tv-eyebrow" }, "Terminal capability"),
						Y("h2", null, "Full access"),
						Y("p", null, "Use one setting for both Spudex Chat and the Manual terminal.")
					], -1), Y("label", { class: B(["tsx-master-toggle", { danger: x.full_access }]) }, [Y("span", null, V(x.full_access ? "Full access" : "Restricted"), 1), W(Y("input", {
						"onUpdate:modelValue": n[17] ||= (e) => x.full_access = e,
						class: "tv-checkbox",
						type: "checkbox",
						onChange: n[18] ||= (e) => b.value = !0
					}, null, 544), [[cs, x.full_access]])], 2)]),
					Y("div", { class: B(["tsx-policy-notice", { danger: x.full_access }]) }, [Y("strong", null, V(x.full_access ? "Full access is on." : "Restricted mode is on."), 1), X(" " + V(x.full_access ? "Spudex can run any command available to Tater, including shells, installs, network tools, containers, and host-affecting commands." : "Spudex applies its command allow-list and available OS isolation."), 1)], 2),
					x.full_access ? (q(), J("div", Ib, [...n[46] ||= [Y("strong", null, "Use with care.", -1), X(" Commands may read environment credentials, change or delete host files, install software, control applications, and contact external services.", -1)]])) : Z("", !0),
					n[47] ||= Y("div", { class: "tsx-guardrails" }, [
						Y("span", null, [
							X("Commands start inside "),
							Y("code", null, "agent_lab"),
							X(".")
						]),
						Y("span", null, "Agent Lab is a starting folder, not a filesystem boundary."),
						Y("span", null, "Processes stay tracked and stoppable.")
					], -1)
				]),
				Y("div", Lb, [n[48] ||= Y("span", null, "Model routing remains in Settings → Models.", -1), Y("button", {
					class: "tv-button primary",
					type: "button",
					disabled: g.value === "settings" || !b.value,
					onClick: ke
				}, V(g.value === "settings" ? "Saving…" : "Save settings"), 9, Rb)])
			]))
		]), ga(kl, {
			open: y.value,
			onClose: n[20] ||= (e) => y.value = !1
		}, {
			default: Sn(() => [Y("section", zb, [Y("header", null, [Y("div", null, [n[49] ||= Y("span", { class: "tv-eyebrow" }, "Session details", -1), Y("h2", null, V(A.value?.label || A.value?.command || "No session selected"), 1)]), Y("button", {
				class: "tv-button",
				type: "button",
				onClick: n[19] ||= (e) => y.value = !1
			}, "Close")]), A.value ? (q(), J("div", Bb, [
				A.value.last_policy_block ? (q(), J("div", Vb, [
					Y("strong", null, V(A.value.last_policy_block.title || "Command blocked"), 1),
					X(" " + V(A.value.last_policy_block.reason || A.value.last_policy_block.message), 1),
					A.value.last_policy_block.toggle ? (q(), J("small", Hb, "Policy toggle: " + V(A.value.last_policy_block.toggle), 1)) : Z("", !0)
				])) : Z("", !0),
				Y("article", null, [n[50] ||= Y("h3", null, "Plan", -1), A.value.plan?.length ? (q(), J("ol", Ub, [(q(!0), J(K, null, G(A.value.plan, (e) => (q(), J("li", {
					key: e.step,
					class: B(z(e.status))
				}, [Y("span", null, V(e.step || "Step"), 1), Y("small", null, [X(V(String(e.status || "pending").replaceAll("_", " ")), 1), e.detail ? (q(), J(K, { key: 0 }, [X(" · " + V(e.detail), 1)], 64)) : Z("", !0)])], 2))), 128))])) : (q(), J("div", Wb, "No task plan yet."))]),
				Y("article", null, [n[51] ||= Y("h3", null, "Verification", -1), A.value.verification ? (q(), J("div", {
					key: 0,
					class: B(["tsx-verification", z(A.value.verification.status)])
				}, [
					Y("strong", null, V(A.value.verification.status === "passed" ? "Verification passed" : A.value.verification.status === "failed" ? "Verification failed" : "Verification recorded"), 1),
					Y("small", null, V(A.value.verification.command), 1),
					A.value.verification.summary ? (q(), J("pre", Gb, V(A.value.verification.summary), 1)) : Z("", !0)
				], 2)) : (q(), J("div", Kb, "No verification run yet."))]),
				Y("article", null, [n[52] ||= Y("h3", null, "App previews", -1), A.value.previews?.length ? (q(), J("div", qb, [(q(!0), J(K, null, G(A.value.previews.slice(-6).reverse(), (e) => (q(), J("a", {
					key: e.url,
					href: e.url,
					target: "_blank",
					rel: "noreferrer"
				}, [Y("span", null, V(e.url), 1), Y("small", null, V(e.source || "preview"), 1)], 8, Jb))), 128))])) : (q(), J("div", Yb, "No app previews detected yet."))]),
				Y("article", null, [n[53] ||= Y("h3", null, "Git", -1), w.value.git?.ok ? (q(), J("div", Xb, [
					Y("div", null, [Y("strong", null, V(w.value.git.branch || "detached"), 1), Y("small", null, V(w.value.git.repo), 1)]),
					Y("span", { class: B(["tv-state", { good: !w.value.git.dirty }]) }, V(w.value.git.dirty ? `${w.value.git.changed_count || w.value.git.changed_files?.length || 0} changed` : "Clean"), 3),
					w.value.git.changed_files?.length ? (q(), J("pre", Zb, V(w.value.git.changed_files.slice(0, 24).join("\n")), 1)) : Z("", !0)
				])) : (q(), J("div", Qb, "No Git repository detected."))]),
				Y("article", $b, [n[54] ||= Y("h3", null, "File changes", -1), A.value.file_changes?.length ? (q(), J("div", ex, [(q(!0), J(K, null, G(A.value.file_changes.slice(-6).reverse(), (e) => (q(), J("section", {
					key: e.id,
					class: B({
						pending: e.pending,
						applied: e.applied
					})
				}, [Y("header", null, [Y("div", null, [Y("strong", null, V(e.path_display || e.path || "File change"), 1), Y("small", null, [X(V(e.pending ? "Pending" : e.applied ? "Applied" : "Rejected"), 1), e.bytes ? (q(), J(K, { key: 0 }, [X(" · " + V(e.bytes) + " bytes", 1)], 64)) : Z("", !0)])]), e.pending ? (q(), J("div", tx, [Y("button", {
					class: "tv-button",
					type: "button",
					onClick: (t) => Oe(String(A.value.id), String(e.id), "approve")
				}, "Approve", 8, nx), Y("button", {
					class: "tv-button danger",
					type: "button",
					onClick: (t) => Oe(String(A.value.id), String(e.id), "reject")
				}, "Reject", 8, rx)])) : Z("", !0)]), Y("pre", null, V(e.diff || "No textual diff available."), 1)], 2))), 128))])) : (q(), J("div", ix, "No file changes yet."))]),
				Y("article", ax, [n[55] ||= Y("h3", null, "Session memory", -1), A.value.memory_summary ? (q(), J("p", ox, V(A.value.memory_summary), 1)) : (q(), J("div", sx, "No session memory yet."))])
			])) : (q(), J("div", cx, "Select a session to see its details."))])]),
			_: 1
		}, 8, ["open"])], 64));
	}
}), ux = { class: "tset-resource" }, dx = { class: "tset-resource-grid" }, fx = { class: "tv-panel tset-form-card" }, px = { class: "tv-form-grid" }, mx = { class: "tv-toggle" }, hx = { class: "full" }, gx = { class: "tset-input-action" }, _x = ["type", "placeholder"], vx = { class: "tv-toggle" }, yx = { class: "tset-endpoints" }, bx = { class: "tv-panel tset-form-card" }, xx = { class: "tset-admin-select" }, Sx = ["value"], Cx = { class: "tv-panel tset-form-card tset-danger-card" }, wx = ["disabled"], Tx = { class: "tset-save-bar" }, Ex = ["disabled"], Dx = /* @__PURE__ */ sr({
	__name: "AdvancedSettings",
	props: {
		settings: {},
		endpoint: {},
		clearChatEndpoint: {},
		chatApiUrl: {},
		modelsApiUrl: {}
	},
	emits: ["saved", "notify"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ Et({
			tater_api_enabled: !1,
			tater_api_mode: "direct",
			tater_api_hydra_tools_enabled: !1
		}), a = /* @__PURE__ */ U(""), o = /* @__PURE__ */ U(!1), s = /* @__PURE__ */ U([]), c = /* @__PURE__ */ U(!1), l = /* @__PURE__ */ U(!1), u = /* @__PURE__ */ U(!1), d = /* @__PURE__ */ U(""), f = /* @__PURE__ */ U(""), p = Q(() => [...new Set([
			...Array.isArray(n.settings.admin_plugin_options) ? n.settings.admin_plugin_options : [],
			...Array.isArray(n.settings.admin_only_plugins) ? n.settings.admin_only_plugins : [],
			...Array.isArray(n.settings.admin_only_plugins_defaults) ? n.settings.admin_only_plugins_defaults : []
		].map((e) => String(e || "").trim()).filter(Boolean))].sort((e, t) => e.localeCompare(t))), m = Q(() => a.value ? "A replacement key is ready. Save to activate it." : n.settings.tater_api_key_set ? "A key is saved. Tater will never show it back here." : "No key is saved yet.");
		function h(e) {
			Object.assign(i, {
				tater_api_enabled: !!e.tater_api_enabled,
				tater_api_mode: String(e.tater_api_mode || "direct"),
				tater_api_hydra_tools_enabled: !!e.tater_api_hydra_tools_enabled
			}), s.value = Array.isArray(e.admin_only_plugins) ? e.admin_only_plugins.map((e) => String(e || "").trim()).filter(Boolean) : [], a.value = "", o.value = !1, u.value = !1;
		}
		function g() {
			u.value = !0, d.value = "", f.value = "";
		}
		function _() {
			let e = /* @__PURE__ */ new Uint8Array(32);
			window.crypto?.getRandomValues ? window.crypto.getRandomValues(e) : e.forEach((t, n) => {
				e[n] = Math.floor(Math.random() * 256);
			}), a.value = `tater-${Array.from(e).map((e) => e.toString(16).padStart(2, "0")).join("")}`, o.value = !0, g();
		}
		function v() {
			s.value = Array.isArray(n.settings.admin_only_plugins_defaults) ? [...n.settings.admin_only_plugins_defaults] : [], g();
		}
		async function y() {
			c.value = !0, d.value = "", f.value = "";
			let e = {
				tater_api_enabled: !!i.tater_api_enabled,
				tater_api_mode: i.tater_api_mode,
				tater_api_hydra_tools_enabled: !!i.tater_api_hydra_tools_enabled,
				admin_only_plugins: [...s.value]
			};
			a.value.trim() && (e.tater_api_key = a.value.trim());
			try {
				let t = await js(n.endpoint, e);
				r("saved", t), h(t), f.value = "Advanced settings saved and synchronized.", r("notify", f.value, "success");
			} catch (e) {
				d.value = e instanceof Error ? e.message : "Advanced settings could not be saved.", r("notify", d.value, "error");
			} finally {
				c.value = !1;
			}
		}
		async function b() {
			if (window.confirm("Clear chat history and uploaded chat attachments now?")) {
				l.value = !0, d.value = "";
				try {
					await js(n.clearChatEndpoint), f.value = "Chat history and uploaded attachments cleared.", r("notify", f.value, "success");
				} catch (e) {
					d.value = e instanceof Error ? e.message : "Chat history could not be cleared.", r("notify", d.value, "error");
				} finally {
					l.value = !1;
				}
			}
		}
		return On(() => n.settings, (e) => {
			u.value || h(e || {});
		}, { immediate: !0 }), (t, n) => (q(), J("section", ux, [
			f.value || d.value ? (q(), J("div", {
				key: 0,
				class: B(["tv-notice", { error: !!d.value }]),
				"aria-live": "polite"
			}, V(d.value || f.value), 3)) : Z("", !0),
			Y("div", dx, [
				Y("section", fx, [n[12] ||= ba("<header><span class=\"tv-eyebrow\">OpenAI-compatible API</span><h2>Local application access</h2><p>Allow other applications to call Tater’s Base model directly or through Hydra.</p></header><div class=\"tadvanced-api-guide\"><div class=\"tadvanced-api-guide-title\"><span>Quick setup</span><strong>The request’s model chooses the route</strong><small>Send the saved key as <code>Authorization: Bearer …</code></small></div><div><code>tater/base</code><span><strong>Direct</strong><small>Uses the Base LLM without Hydra.</small></span></div><div><code>tater/hydra</code><span><strong>Hydra</strong><small>Uses Hydra; tool access follows the setting below.</small></span></div><p><code>GET /v1/models</code> lists available IDs. Other model IDs use <strong>Default mode</strong>.</p></div>", 2), Y("div", px, [
					Y("label", mx, [W(Y("input", {
						"onUpdate:modelValue": n[0] ||= (e) => i.tater_api_enabled = e,
						class: "tv-checkbox",
						type: "checkbox",
						onChange: g
					}, null, 544), [[cs, i.tater_api_enabled]]), n[5] ||= Y("span", null, [Y("strong", null, "Enable API"), Y("small", null, "When disabled, the v1 endpoints reject requests.")], -1)]),
					Y("label", null, [n[7] ||= X(" Default mode ", -1), W(Y("select", {
						"onUpdate:modelValue": n[1] ||= (e) => i.tater_api_mode = e,
						onChange: g
					}, [...n[6] ||= [Y("option", { value: "direct" }, "Direct Base LLM", -1), Y("option", { value: "hydra" }, "Hydra", -1)]], 544), [[ds, i.tater_api_mode]])]),
					Y("label", hx, [
						n[8] ||= X(" API key ", -1),
						Y("div", gx, [W(Y("input", {
							"onUpdate:modelValue": n[2] ||= (e) => a.value = e,
							type: o.value ? "text" : "password",
							autocomplete: "new-password",
							placeholder: e.settings.tater_api_key_set ? "Enter a new key to replace the saved key" : "Generate a key before enabling clients",
							onInput: g
						}, null, 40, _x), [[hs, a.value]]), Y("button", {
							class: "tv-button",
							type: "button",
							onClick: _
						}, "Generate")]),
						Y("small", null, V(m.value), 1)
					]),
					Y("label", vx, [W(Y("input", {
						"onUpdate:modelValue": n[3] ||= (e) => i.tater_api_hydra_tools_enabled = e,
						class: "tv-checkbox",
						type: "checkbox",
						onChange: g
					}, null, 544), [[cs, i.tater_api_hydra_tools_enabled]]), n[9] ||= Y("span", null, [Y("strong", null, "Hydra tool use"), Y("small", null, "Only applies to requests running in Hydra mode.")], -1)]),
					Y("div", yx, [
						n[10] ||= Y("span", null, "Chat", -1),
						Y("code", null, V(e.chatApiUrl), 1),
						n[11] ||= Y("span", null, "Models", -1),
						Y("code", null, V(e.modelsApiUrl), 1)
					])
				])]),
				Y("section", bx, [
					n[15] ||= Y("header", null, [
						Y("span", { class: "tv-eyebrow" }, "Authorization"),
						Y("h2", null, "Admin tool gating"),
						Y("p", null, "Selected Verbas are limited to linked People marked as administrators.")
					], -1),
					Y("label", xx, [
						n[13] ||= X(" Admin-only plugin IDs ", -1),
						W(Y("select", {
							"onUpdate:modelValue": n[4] ||= (e) => s.value = e,
							multiple: "",
							size: "14",
							onChange: g
						}, [(q(!0), J(K, null, G(p.value, (e) => (q(), J("option", {
							key: e,
							value: e
						}, V(e), 9, Sx))), 128))], 544), [[ds, s.value]]),
						n[14] ||= Y("small", null, "Kernel tools remain admin-only whenever at least one Person is marked as an administrator.", -1)
					]),
					Y("button", {
						class: "tv-button",
						type: "button",
						onClick: v
					}, "Reset to defaults")
				]),
				Y("section", Cx, [n[16] ||= Y("header", null, [
					Y("span", { class: "tv-eyebrow" }, "Destructive maintenance"),
					Y("h2", null, "Clear chat history"),
					Y("p", null, "Deletes stored WebUI messages and uploaded chat attachments.")
				], -1), Y("button", {
					class: "tv-button danger",
					type: "button",
					disabled: l.value,
					onClick: b
				}, V(l.value ? "Clearing…" : "Clear chat history"), 9, wx)])
			]),
			Y("footer", Tx, [Y("div", null, [Y("strong", null, V(u.value ? "Unsaved changes" : "Advanced settings are synchronized"), 1), n[17] ||= Y("span", null, "Secrets are write-only and are never returned by the server.", -1)]), Y("button", {
				class: "tv-button primary",
				type: "button",
				disabled: c.value || !u.value,
				onClick: y
			}, V(c.value ? "Saving…" : "Save advanced settings"), 9, Ex)])
		]));
	}
}), Ox = { class: "tset-general" }, kx = { class: "tset-general-grid" }, Ax = { class: "tv-panel tset-form-card tset-theme-section" }, jx = {
	class: "tset-theme-grid",
	role: "radiogroup",
	"aria-label": "Tater color theme"
}, Mx = ["aria-checked", "onClick"], Nx = {
	class: "tset-theme-swatches",
	"aria-hidden": "true"
}, Px = { class: "tset-theme-copy" }, Fx = {
	class: "tset-theme-check",
	"aria-hidden": "true"
}, Ix = { class: "tv-panel tset-form-card" }, Lx = { class: "tv-form-grid" }, Rx = { class: "tv-toggle tset-inline-toggle" }, zx = { class: "full" }, Bx = { class: "tv-panel tset-form-card" }, Vx = { class: "tv-form-grid" }, Hx = { class: "tset-password-row" }, Ux = ["disabled"], Wx = { class: "tv-panel tset-form-card tset-avatar-section" }, Gx = { class: "tset-avatar-grid" }, Kx = ["src"], qx = {
	key: 1,
	class: "tset-avatar-fallback"
}, Jx = ["src"], Yx = {
	key: 1,
	class: "tset-avatar-fallback"
}, Xx = { class: "tset-save-bar" }, Zx = ["disabled"], Qx = /* @__PURE__ */ sr({
	__name: "GeneralSettings",
	props: {
		settings: {},
		endpoint: {},
		onThemePreview: { type: Function }
	},
	emits: ["saved", "notify"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ Et({
			username: "User",
			show_speed_stats: !1,
			webui_theme: "tater",
			tater_first_name: "Tater",
			tater_last_name: "Totterson",
			tater_personality: ""
		}), a = /* @__PURE__ */ U(""), o = /* @__PURE__ */ U(""), s = /* @__PURE__ */ U(!1), c = /* @__PURE__ */ U(""), l = /* @__PURE__ */ U(""), u = /* @__PURE__ */ U(""), d = /* @__PURE__ */ U(""), f = /* @__PURE__ */ U(!1), p = /* @__PURE__ */ U(!1), m = /* @__PURE__ */ U(!1), h = /* @__PURE__ */ U(!1), g = /* @__PURE__ */ U(""), _ = /* @__PURE__ */ U(""), v = /* @__PURE__ */ U(null), y = /* @__PURE__ */ U(null), b = [
			{
				id: "tater",
				name: "Tater",
				description: "The original roasted orange Tater look.",
				colors: [
					"#d65a1f",
					"#f08345",
					"#202225"
				]
			},
			{
				id: "tater-light",
				name: "Tater Light",
				description: "Warm orange on cream and soft-white surfaces.",
				colors: [
					"#c8531d",
					"#fffaf3",
					"#eadbca"
				]
			},
			{
				id: "blueberry",
				name: "Blueberry",
				description: "Clear blue accents with a cool navy surface.",
				colors: [
					"#4285f4",
					"#70b7ff",
					"#1b2635"
				]
			},
			{
				id: "mint",
				name: "Mint",
				description: "Fresh teal-green accents and forest shadows.",
				colors: [
					"#32b58d",
					"#67d6b1",
					"#182923"
				]
			},
			{
				id: "grape",
				name: "Grape",
				description: "Rich violet accents with deep plum panels.",
				colors: [
					"#9b6cf4",
					"#c18aff",
					"#292036"
				]
			},
			{
				id: "strawberry",
				name: "Strawberry",
				description: "Warm rose accents with berry-toned surfaces.",
				colors: [
					"#e75f91",
					"#ff8fb6",
					"#30202a"
				]
			}
		], x = Q(() => !!n.settings.webui_password_set), S = Q(() => s.value ? "WebUI password will be removed when you save." : a.value || o.value ? "The new WebUI password will be applied when you save." : x.value ? "WebUI password is enabled. Login is required." : "No WebUI password is set. Login is not required.");
		function C(e, t = "") {
			return String(e ?? "").trim() || t;
		}
		function w(e, t) {
			return C(e, t).charAt(0).toUpperCase() || t;
		}
		function T(e) {
			Object.assign(i, {
				username: C(e.username, "User"),
				show_speed_stats: !!e.show_speed_stats,
				webui_theme: C(e.webui_theme, "tater").toLowerCase(),
				tater_first_name: C(e.tater_first_name, "Tater"),
				tater_last_name: C(e.tater_last_name, "Totterson"),
				tater_personality: String(e.tater_personality ?? "")
			}), c.value = C(e.user_avatar), l.value = C(e.tater_avatar), u.value = "", d.value = "", f.value = !1, p.value = !1, s.value = !1, a.value = "", o.value = "", v.value && (v.value.value = ""), y.value && (y.value.value = ""), h.value = !1;
		}
		function E(e) {
			i.webui_theme = e, n.onThemePreview?.(e), D();
		}
		function D() {
			h.value = !0, _.value = "", g.value = "";
		}
		function O() {
			(a.value || o.value) && (s.value = !1), D();
		}
		function k() {
			a.value = "", o.value = "", s.value = !0, D();
		}
		function A(e) {
			return new Promise((t, n) => {
				let r = new FileReader();
				r.onload = () => t(String(r.result || "")), r.onerror = () => n(/* @__PURE__ */ Error("The selected image could not be read.")), r.readAsDataURL(e);
			});
		}
		async function j(e, t) {
			let n = t.target.files?.[0];
			if (n) try {
				let t = await A(n);
				e === "user" ? (c.value = t, u.value = t, f.value = !1) : (l.value = t, d.value = t, p.value = !1), D();
			} catch (e) {
				g.value = e instanceof Error ? e.message : "The selected image could not be read.";
			}
		}
		function M(e) {
			e === "user" ? (c.value = "", u.value = "", f.value = !0, v.value && (v.value.value = "")) : (l.value = "", d.value = "", p.value = !0, y.value && (y.value.value = "")), D();
		}
		async function N() {
			if (g.value = "", _.value = "", a.value || o.value) {
				if (a.value.length < 4) {
					g.value = "WebUI password must be at least 4 characters.";
					return;
				}
				if (a.value !== o.value) {
					g.value = "WebUI password confirmation does not match.";
					return;
				}
			}
			let e = {
				username: C(i.username, "User"),
				show_speed_stats: !!i.show_speed_stats,
				webui_theme: i.webui_theme,
				tater_first_name: C(i.tater_first_name, "Tater"),
				tater_last_name: C(i.tater_last_name, "Totterson"),
				tater_personality: i.tater_personality
			};
			s.value ? e.clear_webui_password = !0 : a.value && (e.webui_password = a.value, e.webui_password_confirm = o.value), f.value ? e.clear_user_avatar = !0 : u.value && (e.user_avatar = u.value), p.value ? e.clear_tater_avatar = !0 : d.value && (e.tater_avatar = d.value), m.value = !0;
			try {
				let t = await js(n.endpoint, e);
				r("saved", t), T(t), _.value = "General settings saved and synchronized.", r("notify", _.value, "success");
			} catch (e) {
				g.value = e instanceof Error ? e.message : "General settings could not be saved.", r("notify", g.value, "error");
			} finally {
				m.value = !1;
			}
		}
		return On(() => n.settings, (e) => {
			h.value || T(e || {});
		}, { immediate: !0 }), kr(() => {
			h.value && n.onThemePreview?.(C(n.settings.webui_theme, "tater").toLowerCase());
		}), (e, t) => (q(), J("section", Ox, [
			_.value || g.value ? (q(), J("div", {
				key: 0,
				class: B(["tv-notice", { error: !!g.value }]),
				"aria-live": "polite"
			}, V(g.value || _.value), 3)) : Z("", !0),
			Y("div", kx, [
				Y("section", Ax, [t[11] ||= Y("header", null, [
					Y("span", { class: "tv-eyebrow" }, "Appearance"),
					Y("h2", null, "Color theme"),
					Y("p", null, "Choose a palette for the whole Tater WebUI. Changes preview immediately and are kept when you save.")
				], -1), Y("div", jx, [(q(), J(K, null, G(b, (e) => Y("button", {
					key: e.id,
					class: B(["tset-theme-card", { active: i.webui_theme === e.id }]),
					type: "button",
					role: "radio",
					"aria-checked": i.webui_theme === e.id,
					onClick: (t) => E(e.id)
				}, [
					Y("span", Nx, [(q(!0), J(K, null, G(e.colors, (e) => (q(), J("i", {
						key: e,
						style: R({ background: e })
					}, null, 4))), 128))]),
					Y("span", Px, [Y("strong", null, V(e.name), 1), Y("small", null, V(e.description), 1)]),
					Y("span", Fx, V(i.webui_theme === e.id ? "✓" : ""), 1)
				], 10, Mx)), 64))])]),
				Y("section", Ix, [t[17] ||= Y("header", null, [
					Y("span", { class: "tv-eyebrow" }, "Identity"),
					Y("h2", null, "Names and personality"),
					Y("p", null, "These values are shared by Chat and every surface that refers to you or Tater.")
				], -1), Y("div", Lx, [
					Y("label", null, [t[12] ||= X(" WebUI username ", -1), W(Y("input", {
						"onUpdate:modelValue": t[0] ||= (e) => i.username = e,
						type: "text",
						autocomplete: "username",
						onInput: D
					}, null, 544), [[$, i.username]])]),
					Y("label", Rx, [W(Y("input", {
						"onUpdate:modelValue": t[1] ||= (e) => i.show_speed_stats = e,
						class: "tv-checkbox",
						type: "checkbox",
						onChange: D
					}, null, 544), [[cs, i.show_speed_stats]]), t[13] ||= Y("span", null, [Y("strong", null, "Show tokens/sec stats"), Y("small", null, "Display generation speed in Chat.")], -1)]),
					Y("label", null, [t[14] ||= X(" Tater first name ", -1), W(Y("input", {
						"onUpdate:modelValue": t[2] ||= (e) => i.tater_first_name = e,
						type: "text",
						onInput: D
					}, null, 544), [[$, i.tater_first_name]])]),
					Y("label", null, [t[15] ||= X(" Tater last name ", -1), W(Y("input", {
						"onUpdate:modelValue": t[3] ||= (e) => i.tater_last_name = e,
						type: "text",
						onInput: D
					}, null, 544), [[$, i.tater_last_name]])]),
					Y("label", zx, [t[16] ||= X(" Personality / style ", -1), W(Y("textarea", {
						"onUpdate:modelValue": t[4] ||= (e) => i.tater_personality = e,
						rows: "5",
						onInput: D
					}, null, 544), [[$, i.tater_personality]])])
				])]),
				Y("section", Bx, [
					t[20] ||= Y("header", null, [
						Y("span", { class: "tv-eyebrow" }, "Access"),
						Y("h2", null, "WebUI login"),
						Y("p", null, "Leave both password fields blank to keep the current login setting.")
					], -1),
					Y("div", Vx, [Y("label", null, [t[18] ||= X(" New password ", -1), W(Y("input", {
						"onUpdate:modelValue": t[5] ||= (e) => a.value = e,
						type: "password",
						autocomplete: "new-password",
						onInput: O
					}, null, 544), [[$, a.value]])]), Y("label", null, [t[19] ||= X(" Repeat password ", -1), W(Y("input", {
						"onUpdate:modelValue": t[6] ||= (e) => o.value = e,
						type: "password",
						autocomplete: "new-password",
						onInput: O
					}, null, 544), [[$, o.value]])])]),
					Y("div", Hx, [Y("button", {
						class: "tv-button danger",
						type: "button",
						disabled: !x.value || !!(a.value || o.value),
						onClick: k
					}, " Remove password ", 8, Ux), Y("span", null, V(S.value), 1)])
				]),
				Y("section", Wx, [t[23] ||= Y("header", null, [
					Y("span", { class: "tv-eyebrow" }, "Appearance"),
					Y("h2", null, "Chat avatars"),
					Y("p", null, "Previews update immediately; the new images become authoritative when saved.")
				], -1), Y("div", Gx, [Y("article", null, [
					t[21] ||= Y("span", null, "WebUI user", -1),
					c.value ? (q(), J("img", {
						key: 0,
						src: c.value,
						alt: "WebUI user avatar preview"
					}, null, 8, Kx)) : (q(), J("div", qx, V(w(i.username, "U")), 1)),
					Y("input", {
						ref_key: "userAvatarInput",
						ref: v,
						type: "file",
						accept: "image/png,image/jpeg,image/gif,image/webp",
						onChange: t[7] ||= (e) => j("user", e)
					}, null, 544),
					Y("button", {
						class: "tv-button danger",
						type: "button",
						onClick: t[8] ||= (e) => M("user")
					}, "Clear avatar")
				]), Y("article", null, [
					t[22] ||= Y("span", null, "Tater", -1),
					l.value ? (q(), J("img", {
						key: 0,
						src: l.value,
						alt: "Tater avatar preview"
					}, null, 8, Jx)) : (q(), J("div", Yx, V(w(i.tater_first_name, "T")), 1)),
					Y("input", {
						ref_key: "taterAvatarInput",
						ref: y,
						type: "file",
						accept: "image/png,image/jpeg,image/gif,image/webp",
						onChange: t[9] ||= (e) => j("tater", e)
					}, null, 544),
					Y("button", {
						class: "tv-button danger",
						type: "button",
						onClick: t[10] ||= (e) => M("tater")
					}, "Clear avatar")
				])])])
			]),
			Y("footer", Xx, [Y("div", null, [Y("strong", null, V(h.value ? "Unsaved changes" : "General settings are synchronized"), 1), t[24] ||= Y("span", null, "Saving updates every Vue surface from the canonical server response.", -1)]), Y("button", {
				class: "tv-button primary",
				type: "button",
				disabled: m.value || !h.value,
				onClick: N
			}, V(m.value ? "Saving…" : "Save general settings"), 9, Zx)])
		]));
	}
}), $x = { class: "tset-resource thydra" }, eS = {
	class: "tv-tabs thydra-tabs",
	"aria-label": "Hydra settings sections"
}, tS = { class: "tset-resource-grid" }, nS = { class: "tv-panel tset-form-card" }, rS = { class: "tv-form-grid" }, iS = { class: "tv-panel tset-form-card" }, aS = { class: "tv-form-grid" }, oS = { class: "tv-toggle" }, sS = { class: "tv-toggle" }, cS = { class: "tset-save-bar" }, lS = ["disabled"], uS = { class: "tv-panel tset-form-card" }, dS = { class: "thydra-panel-head" }, fS = ["disabled"], pS = { class: "tv-form-grid thydra-filters" }, mS = ["value"], hS = ["value"], gS = { class: "tv-toggle" }, _S = {
	key: 0,
	class: "thydra-stack"
}, vS = { class: "tv-panel tset-form-card" }, yS = { class: "thydra-counter-groups" }, bS = { class: "tv-metrics thydra-metrics" }, xS = { class: "tv-metrics thydra-metrics" }, SS = { class: "tv-panel tset-form-card" }, CS = { class: "thydra-rate-grid" }, wS = { class: "tv-panel tset-form-card" }, TS = { class: "thydra-table-wrap" }, ES = { class: "tv-panel tset-form-card" }, DS = {
	key: 0,
	class: "thydra-ledger"
}, OS = {
	key: 1,
	class: "tv-empty"
}, kS = { class: "tset-resource-grid" }, AS = {
	key: 0,
	class: "thydra-bars"
}, jS = {
	key: 1,
	class: "tv-empty"
}, MS = { class: "tv-panel tset-form-card" }, NS = { class: "thydra-panel-head" }, PS = ["disabled"], FS = { class: "tv-metrics thydra-data-summary" }, IS = {
	key: 0,
	class: "thydra-stack"
}, LS = { class: "tset-resource-grid" }, RS = {
	key: 0,
	class: "thydra-bars"
}, zS = {
	key: 1,
	class: "tv-empty"
}, BS = { class: "tv-panel tset-form-card" }, VS = { class: "thydra-table-wrap" }, HS = { class: "tv-panel tset-form-card tset-danger-card thydra-clear-card" }, US = ["value"], WS = { class: "thydra-clear-actions" }, GS = ["disabled"], KS = ["disabled"], qS = ["disabled"], JS = ["disabled"], YS = /* @__PURE__ */ sr({
	__name: "HydraSettings",
	props: {
		settings: {},
		endpoint: {},
		metricsEndpoint: {},
		dataEndpoint: {},
		clearDataEndpoint: {}
	},
	emits: ["saved", "notify"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ U("settings"), a = /* @__PURE__ */ U(!1), o = /* @__PURE__ */ U(!1), s = /* @__PURE__ */ U(""), c = /* @__PURE__ */ U(""), l = /* @__PURE__ */ U(null), u = /* @__PURE__ */ U(null), d = /* @__PURE__ */ U(!1), f = /* @__PURE__ */ U(!1), p = /* @__PURE__ */ U(!1), m = /* @__PURE__ */ Et({
			max_display: 8,
			max_store: 20,
			max_llm: 8,
			hydra_max_ledger_items: 1500,
			hydra_astraeus_plan_review_enabled: !0,
			hydra_auto_continue_incomplete_final_enabled: !1
		}), h = /* @__PURE__ */ Et({
			platform: "all",
			limit: 50,
			outcome: "all",
			tool: "all",
			toolsOnly: !1
		}), g = /* @__PURE__ */ U("webui"), _ = Q(() => Array.isArray(l.value?.metric_names) ? l.value?.metric_names : []), v = Q(() => ["all", ...(Array.isArray(l.value?.tool_options) ? l.value?.tool_options : []).map((e) => String(e || "")).filter(Boolean)]), y = Q(() => {
			let e = Array.isArray(l.value?.platform_options) ? l.value?.platform_options : [], t = Array.isArray(u.value?.platform_options) ? u.value?.platform_options : [], n = new Set([...e, ...t].map((e) => String(e || "")).filter(Boolean));
			return n.delete("all"), [...n];
		}), b = Q(() => Array.isArray(l.value?.summary_rows) ? l.value?.summary_rows : []), x = Q(() => u.value?.summary && typeof u.value.summary == "object" ? u.value.summary : {});
		function S(e, t, n, r) {
			let i = Number(e);
			if (!Number.isFinite(i)) return t;
			let a = Math.round(i);
			return Math.min(r ?? a, Math.max(n, a));
		}
		function C(e) {
			Object.assign(m, {
				max_display: S(e.max_display, 8, 1),
				max_store: S(e.max_store, 20, 0),
				max_llm: S(e.max_llm, 8, 1),
				hydra_max_ledger_items: S(e.hydra_max_ledger_items, 1500, 1),
				hydra_astraeus_plan_review_enabled: !!e.hydra_astraeus_plan_review_enabled,
				hydra_auto_continue_incomplete_final_enabled: !!e.hydra_auto_continue_incomplete_final_enabled
			}), o.value = !1;
		}
		function w() {
			o.value = !0, s.value = "", c.value = "";
		}
		function T() {
			let e = n.settings.defaults || {};
			Object.assign(m, {
				max_display: S(e.max_display, 8, 1),
				max_store: S(e.max_store, 20, 0),
				max_llm: S(e.max_llm, 8, 1),
				hydra_max_ledger_items: S(e.hydra_max_ledger_items, 1500, 1),
				hydra_astraeus_plan_review_enabled: e.hydra_astraeus_plan_review_enabled !== !1,
				hydra_auto_continue_incomplete_final_enabled: !!e.hydra_auto_continue_incomplete_final_enabled
			}), w(), s.value = "Default behavior values loaded. Save to apply them.";
		}
		async function E() {
			a.value = !0, c.value = "", s.value = "";
			let e = {
				max_display: S(m.max_display, 8, 1),
				max_store: S(m.max_store, 20, 0),
				max_llm: S(m.max_llm, 8, 1),
				hydra_max_ledger_items: S(m.hydra_max_ledger_items, 1500, 1),
				hydra_astraeus_plan_review_enabled: !!m.hydra_astraeus_plan_review_enabled,
				hydra_auto_continue_incomplete_final_enabled: !!m.hydra_auto_continue_incomplete_final_enabled
			};
			try {
				let t = await js(n.endpoint, e);
				r("saved", t), C(t), s.value = "Hydra behavior saved and synchronized.", r("notify", s.value, "success");
			} catch (e) {
				c.value = e instanceof Error ? e.message : "Hydra behavior could not be saved.", r("notify", c.value, "error");
			} finally {
				a.value = !1;
			}
		}
		function D(e) {
			return String(e || "").replaceAll("_", " ").replace(/\b\w/g, (e) => e.toUpperCase());
		}
		function O(e) {
			let t = String(e || "").trim();
			return t ? t === "all" ? "All portals" : D(t) : "Unknown";
		}
		function k(e) {
			let t = Number(e);
			return Number.isFinite(t) ? t.toFixed(4) : "0.0000";
		}
		function A(e) {
			return Array.isArray(e) ? e : [];
		}
		function j(e, t) {
			let n = A(e), r = Math.max(1, ...n.map((e) => Number(e.value || 0)));
			return `${Math.max(2, Number(t || 0) / r * 100)}%`;
		}
		async function M() {
			d.value = !0, c.value = "";
			let e = new URLSearchParams({
				platform: h.platform,
				limit: String(S(h.limit, 50, 10, 300)),
				outcome: h.outcome,
				tool: h.tool,
				show_only_tool_turns: h.toolsOnly ? "true" : "false"
			});
			try {
				let t = await As(`${n.metricsEndpoint}?${e.toString()}`);
				l.value = t, v.value.includes(h.tool) || (h.tool = "all");
			} catch (e) {
				c.value = e instanceof Error ? e.message : "Hydra metrics could not be loaded.";
			} finally {
				d.value = !1;
			}
		}
		async function N() {
			f.value = !0, c.value = "";
			try {
				u.value = await As(n.dataEndpoint), !y.value.includes(g.value) && y.value.length && (g.value = y.value[0]);
			} catch (e) {
				c.value = e instanceof Error ? e.message : "Hydra data could not be loaded.";
			} finally {
				f.value = !1;
			}
		}
		async function P(e, t) {
			let i = t === "all" ? "every portal" : O(t), a = e === "all" ? "metrics and ledger data" : e;
			if (window.confirm(`Clear Hydra ${a} for ${i}? This cannot be undone.`)) {
				p.value = !0, c.value = "";
				try {
					let i = await js(n.clearDataEndpoint, {
						mode: e,
						platform: t
					});
					s.value = `Cleared ${Number(i.metrics_removed || 0)} metric keys and ${Number(i.ledger_removed || 0)} ledger lists.`, r("notify", s.value, "success"), await Promise.all([N(), M()]);
				} catch (e) {
					c.value = e instanceof Error ? e.message : "Hydra data could not be cleared.", r("notify", c.value, "error");
				} finally {
					p.value = !1;
				}
			}
		}
		function F(e) {
			i.value = e, e === "metrics" && !l.value && M(), e === "data" && !u.value && N();
		}
		return On(() => n.settings, (e) => {
			o.value || C(e || {});
		}, { immediate: !0 }), Er(() => {
			Promise.allSettled([M(), N()]);
		}), (e, t) => (q(), J("section", $x, [
			s.value || c.value ? (q(), J("div", {
				key: 0,
				class: B(["tv-notice", { error: !!c.value }]),
				"aria-live": "polite"
			}, V(c.value || s.value), 3)) : Z("", !0),
			Y("nav", eS, [
				Y("button", {
					type: "button",
					class: B({ active: i.value === "settings" }),
					onClick: t[0] ||= (e) => F("settings")
				}, "Behavior", 2),
				Y("button", {
					type: "button",
					class: B({ active: i.value === "metrics" }),
					onClick: t[1] ||= (e) => F("metrics")
				}, "Metrics", 2),
				Y("button", {
					type: "button",
					class: B({ active: i.value === "data" }),
					onClick: t[2] ||= (e) => F("data")
				}, "Stored data", 2)
			]),
			i.value === "settings" ? (q(), J(K, { key: 1 }, [Y("div", tS, [Y("section", nS, [t[24] ||= Y("header", null, [
				Y("span", { class: "tv-eyebrow" }, "Conversation windows"),
				Y("h2", null, "History and context"),
				Y("p", null, "Control how much conversation state is displayed, retained, and sent to the active model.")
			], -1), Y("div", rS, [
				Y("label", null, [t[19] ||= X("Messages shown in WebUI", -1), W(Y("input", {
					"onUpdate:modelValue": t[3] ||= (e) => m.max_display = e,
					type: "number",
					min: "1",
					onInput: w
				}, null, 544), [[
					$,
					m.max_display,
					void 0,
					{ number: !0 }
				]])]),
				Y("label", null, [
					t[20] ||= X("Maximum stored messages", -1),
					W(Y("input", {
						"onUpdate:modelValue": t[4] ||= (e) => m.max_store = e,
						type: "number",
						min: "0",
						onInput: w
					}, null, 544), [[
						$,
						m.max_store,
						void 0,
						{ number: !0 }
					]]),
					t[21] ||= Y("small", null, "Use 0 for unlimited storage.", -1)
				]),
				Y("label", null, [t[22] ||= X("Messages sent to LLM", -1), W(Y("input", {
					"onUpdate:modelValue": t[5] ||= (e) => m.max_llm = e,
					type: "number",
					min: "1",
					onInput: w
				}, null, 544), [[
					$,
					m.max_llm,
					void 0,
					{ number: !0 }
				]])]),
				Y("label", null, [t[23] ||= X("Maximum ledger items", -1), W(Y("input", {
					"onUpdate:modelValue": t[6] ||= (e) => m.hydra_max_ledger_items = e,
					type: "number",
					min: "1",
					onInput: w
				}, null, 544), [[
					$,
					m.hydra_max_ledger_items,
					void 0,
					{ number: !0 }
				]])])
			])]), Y("section", iS, [
				t[27] ||= Y("header", null, [
					Y("span", { class: "tv-eyebrow" }, "Planning"),
					Y("h2", null, "Failure handling"),
					Y("p", null, "Choose whether Hydra performs extra planning and recovery passes.")
				], -1),
				Y("div", aS, [Y("label", oS, [W(Y("input", {
					"onUpdate:modelValue": t[7] ||= (e) => m.hydra_astraeus_plan_review_enabled = e,
					class: "tv-checkbox",
					type: "checkbox",
					onChange: w
				}, null, 544), [[cs, m.hydra_astraeus_plan_review_enabled]]), t[25] ||= Y("span", null, [Y("strong", null, "Astraeus second plan check"), Y("small", null, "May improve planning quality at the cost of latency.")], -1)]), Y("label", sS, [W(Y("input", {
					"onUpdate:modelValue": t[8] ||= (e) => m.hydra_auto_continue_incomplete_final_enabled = e,
					class: "tv-checkbox",
					type: "checkbox",
					onChange: w
				}, null, 544), [[cs, m.hydra_auto_continue_incomplete_final_enabled]]), t[26] ||= Y("span", null, [Y("strong", null, "Continue incomplete final answers"), Y("small", null, "Automatically request a continuation when a final response appears truncated.")], -1)])]),
				Y("button", {
					class: "tv-button",
					type: "button",
					onClick: T
				}, "Load defaults")
			])]), Y("footer", cS, [Y("div", null, [Y("strong", null, V(o.value ? "Unsaved changes" : "Hydra behavior is synchronized"), 1), t[28] ||= Y("span", null, "Model routing remains under the Models tab.", -1)]), Y("button", {
				class: "tv-button primary",
				type: "button",
				disabled: a.value || !o.value,
				onClick: E
			}, V(a.value ? "Saving…" : "Save Hydra behavior"), 9, lS)])], 64)) : i.value === "metrics" ? (q(), J(K, { key: 2 }, [Y("section", uS, [Y("header", dS, [t[29] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Live telemetry"),
				Y("h2", null, "Hydra metrics"),
				Y("p", null, "Filter recent planning, tool, and validation activity.")
			], -1), Y("button", {
				class: "tv-button",
				type: "button",
				disabled: d.value,
				onClick: M
			}, V(d.value ? "Refreshing…" : "Refresh"), 9, fS)]), Y("div", pS, [
				Y("label", null, [t[31] ||= X("Portal", -1), W(Y("select", {
					"onUpdate:modelValue": t[9] ||= (e) => h.platform = e,
					onChange: M
				}, [t[30] ||= Y("option", { value: "all" }, "All portals", -1), (q(!0), J(K, null, G(y.value, (e) => (q(), J("option", {
					key: e,
					value: e
				}, V(O(e)), 9, mS))), 128))], 544), [[ds, h.platform]])]),
				Y("label", null, [t[32] ||= X("Ledger entries", -1), W(Y("input", {
					"onUpdate:modelValue": t[10] ||= (e) => h.limit = e,
					type: "number",
					min: "10",
					max: "300",
					step: "10",
					onChange: M
				}, null, 544), [[
					$,
					h.limit,
					void 0,
					{ number: !0 }
				]])]),
				Y("label", null, [t[34] ||= X("Outcome", -1), W(Y("select", {
					"onUpdate:modelValue": t[11] ||= (e) => h.outcome = e,
					onChange: M
				}, [...t[33] ||= [
					Y("option", { value: "all" }, "All", -1),
					Y("option", { value: "done" }, "Done", -1),
					Y("option", { value: "blocked" }, "Blocked", -1),
					Y("option", { value: "failed" }, "Failed", -1)
				]], 544), [[ds, h.outcome]])]),
				Y("label", null, [t[35] ||= X("Tool", -1), W(Y("select", {
					"onUpdate:modelValue": t[12] ||= (e) => h.tool = e,
					onChange: M
				}, [(q(!0), J(K, null, G(v.value, (e) => (q(), J("option", {
					key: e,
					value: e
				}, V(e), 9, hS))), 128))], 544), [[ds, h.tool]])]),
				Y("label", gS, [W(Y("input", {
					"onUpdate:modelValue": t[13] ||= (e) => h.toolsOnly = e,
					class: "tv-checkbox",
					type: "checkbox",
					onChange: M
				}, null, 544), [[cs, h.toolsOnly]]), t[36] ||= Y("span", null, [Y("strong", null, "Tool turns only"), Y("small", null, "Hide ledger entries without a planned tool.")], -1)])
			])]), l.value ? (q(), J("div", _S, [
				Y("section", vS, [Y("header", null, [
					t[37] ||= Y("span", { class: "tv-eyebrow" }, "Counters", -1),
					t[38] ||= Y("h2", null, "Global and selected portal", -1),
					Y("p", null, "Showing " + V(Number(l.value.ledger_filtered || 0)) + " of " + V(Number(l.value.ledger_total || 0)) + " recent ledger rows.", 1)
				]), Y("div", yS, [Y("div", null, [t[39] ||= Y("h3", null, "Global", -1), Y("div", bS, [(q(!0), J(K, null, G(_.value, (e) => (q(), J("div", { key: `global-${e}` }, [Y("span", null, V(D(e)), 1), Y("strong", null, V(Number(l.value.global_metrics?.[e] || 0)), 1)]))), 128))])]), Y("div", null, [Y("h3", null, V(l.value.selected_platform_label || O(h.platform)), 1), Y("div", xS, [(q(!0), J(K, null, G(_.value, (e) => (q(), J("div", { key: `portal-${e}` }, [Y("span", null, V(D(e)), 1), Y("strong", null, V(Number(l.value.platform_metrics?.[e] || 0)), 1)]))), 128))])])])]),
				Y("section", SS, [t[42] ||= Y("header", null, [Y("span", { class: "tv-eyebrow" }, "Rates"), Y("h2", null, "Execution quality")], -1), Y("div", CS, [Y("div", null, [t[40] ||= Y("h3", null, "Global", -1), Y("dl", null, [(q(!0), J(K, null, G(A(l.value.global_rates), (e) => (q(), J(K, { key: e.metric }, [Y("dt", null, V(D(e.metric)), 1), Y("dd", null, V(k(e.value)), 1)], 64))), 128))])]), Y("div", null, [t[41] ||= Y("h3", null, "Selected portal", -1), Y("dl", null, [(q(!0), J(K, null, G(A(l.value.platform_rates), (e) => (q(), J(K, { key: e.metric }, [Y("dt", null, V(D(e.metric)), 1), Y("dd", null, V(k(e.value)), 1)], 64))), 128))])])])]),
				Y("section", wS, [t[44] ||= Y("header", null, [Y("span", { class: "tv-eyebrow" }, "Portal comparison"), Y("h2", null, "Per-portal totals")], -1), Y("div", TS, [Y("table", null, [t[43] ||= Y("thead", null, [Y("tr", null, [
					Y("th", null, "Portal"),
					Y("th", null, "Turns"),
					Y("th", null, "Tools"),
					Y("th", null, "Repairs"),
					Y("th", null, "Validation failures"),
					Y("th", null, "Tool failures"),
					Y("th", null, "Tool rate"),
					Y("th", null, "Repair rate")
				])], -1), Y("tbody", null, [(q(!0), J(K, null, G(A(l.value.platform_rows), (e) => (q(), J("tr", { key: e.platform }, [
					Y("td", null, V(e.platform_label || O(e.platform)), 1),
					Y("td", null, V(e.total_turns || 0), 1),
					Y("td", null, V(e.total_tools_called || 0), 1),
					Y("td", null, V(e.total_repairs || 0), 1),
					Y("td", null, V(e.validation_failures || 0), 1),
					Y("td", null, V(e.tool_failures || 0), 1),
					Y("td", null, V(k(e.tool_call_rate)), 1),
					Y("td", null, V(k(e.repair_rate)), 1)
				]))), 128))])])])]),
				Y("section", ES, [t[49] ||= Y("header", null, [Y("span", { class: "tv-eyebrow" }, "Recent activity"), Y("h2", null, "Ledger")], -1), b.value.length ? (q(), J("div", DS, [(q(!0), J(K, null, G(b.value, (e, n) => (q(), J("details", { key: `${e.time}-${n}` }, [
					Y("summary", null, [
						Y("span", null, V(e.time || "Recent turn"), 1),
						Y("strong", null, V(e.outcome || "unknown"), 1),
						Y("span", null, V(e.platform || ""), 1),
						Y("span", null, V(e.planned_tool || "No tool"), 1),
						Y("span", null, V(e.total_ms || 0) + " ms", 1)
					]),
					Y("dl", null, [
						t[45] ||= Y("dt", null, "Scope", -1),
						Y("dd", null, V(e.scope || "—"), 1),
						t[46] ||= Y("dt", null, "Planner", -1),
						Y("dd", null, V(e.planner_kind || "—"), 1),
						t[47] ||= Y("dt", null, "Validation", -1),
						Y("dd", null, V(e.validation_status || "—") + " " + V(e.validation_reason || ""), 1),
						t[48] ||= Y("dt", null, "Result", -1),
						Y("dd", null, V(e.tool_result_summary || e.outcome_reason || "—"), 1)
					]),
					Y("pre", null, V(JSON.stringify(e.raw || e, null, 2)), 1)
				]))), 128))])) : (q(), J("p", OS, "No ledger rows match these filters."))]),
				Y("div", kS, [(q(!0), J(K, null, G([{
					title: "Top tools",
					items: l.value.top_tools
				}, {
					title: "Top failure reasons",
					items: l.value.top_reasons
				}], (e) => (q(), J("section", {
					key: e.title,
					class: "tv-panel tset-form-card"
				}, [Y("header", null, [t[50] ||= Y("span", { class: "tv-eyebrow" }, "Filtered ledger", -1), Y("h2", null, V(e.title), 1)]), A(e.items).length ? (q(), J("div", AS, [(q(!0), J(K, null, G(A(e.items), (t) => (q(), J("div", { key: t.label }, [
					Y("span", null, V(t.label), 1),
					Y("i", null, [Y("b", { style: R({ width: j(e.items, t.value) }) }, null, 4)]),
					Y("strong", null, V(t.value), 1)
				]))), 128))])) : (q(), J("p", jS, "No matching activity."))]))), 128))])
			])) : Z("", !0)], 64)) : (q(), J(K, { key: 3 }, [Y("section", MS, [Y("header", NS, [t[51] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Storage"),
				Y("h2", null, "Hydra data"),
				Y("p", null, "Inspect or clear metric counters and execution ledgers.")
			], -1), Y("button", {
				class: "tv-button",
				type: "button",
				disabled: f.value,
				onClick: N
			}, V(f.value ? "Refreshing…" : "Refresh"), 9, PS)]), Y("div", FS, [
				Y("div", null, [t[52] ||= Y("span", null, "Metric keys", -1), Y("strong", null, V(Number(x.value.metric_keys || 0)), 1)]),
				Y("div", null, [t[53] ||= Y("span", null, "Ledger lists", -1), Y("strong", null, V(Number(x.value.ledger_lists || 0)), 1)]),
				Y("div", null, [t[54] ||= Y("span", null, "Ledger entries", -1), Y("strong", null, V(Number(x.value.ledger_entries_total || 0)), 1)])
			])]), u.value ? (q(), J("div", IS, [
				Y("div", LS, [(q(!0), J(K, null, G([{
					title: "Turns by portal",
					items: u.value.turns_chart
				}, {
					title: "Ledger entries by key",
					items: u.value.ledger_chart
				}], (e) => (q(), J("section", {
					key: e.title,
					class: "tv-panel tset-form-card"
				}, [Y("header", null, [t[55] ||= Y("span", { class: "tv-eyebrow" }, "Distribution", -1), Y("h2", null, V(e.title), 1)]), A(e.items).length ? (q(), J("div", RS, [(q(!0), J(K, null, G(A(e.items), (t) => (q(), J("div", { key: t.label }, [
					Y("span", null, V(t.label), 1),
					Y("i", null, [Y("b", { style: R({ width: j(e.items, t.value) }) }, null, 4)]),
					Y("strong", null, V(t.value), 1)
				]))), 128))])) : (q(), J("p", zS, "No stored values yet."))]))), 128))]),
				Y("section", BS, [t[57] ||= Y("header", null, [Y("span", { class: "tv-eyebrow" }, "Portal storage"), Y("h2", null, "Stored counters")], -1), Y("div", VS, [Y("table", null, [t[56] ||= Y("thead", null, [Y("tr", null, [
					Y("th", null, "Portal"),
					Y("th", null, "Turns"),
					Y("th", null, "Tools"),
					Y("th", null, "Repairs"),
					Y("th", null, "Validation failures"),
					Y("th", null, "Tool failures"),
					Y("th", null, "Ledger entries")
				])], -1), Y("tbody", null, [(q(!0), J(K, null, G(A(u.value.platform_rows), (e) => (q(), J("tr", { key: e.platform }, [
					Y("td", null, V(e.platform_label || O(e.platform)), 1),
					Y("td", null, V(e.total_turns || 0), 1),
					Y("td", null, V(e.total_tools_called || 0), 1),
					Y("td", null, V(e.total_repairs || 0), 1),
					Y("td", null, V(e.validation_failures || 0), 1),
					Y("td", null, V(e.tool_failures || 0), 1),
					Y("td", null, V(e.ledger_entries || 0), 1)
				]))), 128))])])])]),
				Y("section", HS, [
					t[59] ||= Y("header", null, [
						Y("span", { class: "tv-eyebrow" }, "Destructive maintenance"),
						Y("h2", null, "Clear Hydra data"),
						Y("p", null, "Choose a portal and remove its counters, ledger, or both.")
					], -1),
					Y("label", null, [t[58] ||= X("Portal", -1), W(Y("select", { "onUpdate:modelValue": t[14] ||= (e) => g.value = e }, [(q(!0), J(K, null, G(y.value, (e) => (q(), J("option", {
						key: e,
						value: e
					}, V(O(e)), 9, US))), 128))], 512), [[ds, g.value]])]),
					Y("div", WS, [
						Y("button", {
							class: "tv-button danger",
							type: "button",
							disabled: p.value,
							onClick: t[15] ||= (e) => P("metrics", g.value)
						}, "Reset metrics", 8, GS),
						Y("button", {
							class: "tv-button danger",
							type: "button",
							disabled: p.value,
							onClick: t[16] ||= (e) => P("ledger", g.value)
						}, "Clear ledger", 8, KS),
						Y("button", {
							class: "tv-button danger",
							type: "button",
							disabled: p.value,
							onClick: t[17] ||= (e) => P("all", g.value)
						}, "Clear portal data", 8, qS),
						Y("button", {
							class: "tv-button danger",
							type: "button",
							disabled: p.value,
							onClick: t[18] ||= (e) => P("all", "all")
						}, "Clear everything", 8, JS)
					])
				])
			])) : Z("", !0)], 64))
		]));
	}
}), XS = { class: "tset-resource tlogs" }, ZS = { class: "tv-panel tlogs-console" }, QS = { class: "tlogs-head" }, $S = { class: "tv-metrics tlogs-summary" }, eC = { class: "tlogs-actions" }, tC = ["disabled"], nC = { class: "tlogs-filters" }, rC = ["onKeydown"], iC = { id: "tlogs-logger-options" }, aC = ["value"], oC = { class: "app-log-time" }, sC = { class: "app-log-level" }, cC = { class: "app-log-body" }, lC = { key: 0 }, uC = {
	key: 1,
	class: "app-log-exception"
}, dC = {
	key: 0,
	class: "app-log-empty"
}, fC = /* @__PURE__ */ sr({
	__name: "LogsSettings",
	props: {
		endpoint: {},
		initialAutoScroll: {
			type: Boolean,
			default: !0
		}
	},
	emits: ["notify", "autoScrollChange"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ U([]), a = /* @__PURE__ */ U(0), o = /* @__PURE__ */ U(!1), s = /* @__PURE__ */ U(!1), c = /* @__PURE__ */ U(n.initialAutoScroll), l = /* @__PURE__ */ U(""), u = /* @__PURE__ */ U(""), d = /* @__PURE__ */ U([]), f = /* @__PURE__ */ U({}), p = /* @__PURE__ */ U("Opening live tail…"), m = /* @__PURE__ */ U(""), h = /* @__PURE__ */ U(null), g = !1, _ = 0, v = 0, y = Q(() => i.value.slice(-500)), b = Q(() => Number(f.value.warning || f.value.warn || 0)), x = Q(() => Number(f.value.error || 0) + Number(f.value.critical || 0));
		function S(e) {
			return {
				...e,
				seq: Math.max(0, Number(e.seq || 0)),
				ts: Number(e.ts || 0),
				level: String(e.level || "info").trim().toLowerCase() || "info",
				logger: String(e.logger || "root").trim() || "root",
				module: String(e.module || "").trim(),
				function: String(e.function || "").trim(),
				line: Math.max(0, Number(e.line || 0)),
				message: String(e.message || "").trim(),
				display: String(e.display || e.message || "").trim(),
				exception: String(e.exception || "").trim()
			};
		}
		function C(e) {
			let t = Number(e || 0);
			if (!Number.isFinite(t) || t <= 0) return "--:--:--";
			try {
				return (/* @__PURE__ */ new Date(t * 1e3)).toLocaleTimeString([], {
					hour: "numeric",
					minute: "2-digit",
					second: "2-digit"
				});
			} catch {
				return "--:--:--";
			}
		}
		function w(e) {
			return `${[e.module, e.function].filter(Boolean).join(".")}${e.line ? `:${e.line}` : ""}`;
		}
		function T() {
			let e = h.value;
			return !e || Math.abs(e.scrollHeight - e.clientHeight - e.scrollTop) < 72;
		}
		async function E() {
			await un(), h.value && (h.value.scrollTop = h.value.scrollHeight);
		}
		function D() {
			_ && window.clearTimeout(_), _ = 0;
		}
		function O(e = 1e3) {
			D(), !(!g || s.value) && (_ = window.setTimeout(async () => {
				_ = 0, await k(!1), O(1e3);
			}, Math.max(300, e)));
		}
		async function k(e = !1) {
			let t = ++v, r = c.value && (e || T());
			o.value = !0, m.value = "";
			let s = new URLSearchParams({
				after_seq: String(e ? 0 : a.value),
				limit: "500",
				level: l.value,
				logger_name: u.value.trim()
			});
			try {
				let o = await As(`${n.endpoint}?${s.toString()}`);
				if (!g || t !== v) return;
				let c = (Array.isArray(o.entries) ? o.entries : []).map((e) => S(e)).filter((e) => e.seq > 0);
				if (e) i.value = c;
				else if (c.length) {
					let e = new Map(i.value.map((e) => [e.seq, e]));
					c.forEach((t) => e.set(t.seq, t)), i.value = [...e.values()].sort((e, t) => e.seq - t.seq).slice(-800);
				}
				a.value = Math.max(a.value, Number(o.next_seq || 0)), d.value = Array.isArray(o.loggers) ? o.loggers.map((e) => String(e || "")).filter(Boolean) : [], f.value = o.level_counts && typeof o.level_counts == "object" ? o.level_counts : {}, p.value = `${y.value.length} visible · ${b.value} warnings · ${x.value} errors`, r && await E();
			} catch (e) {
				if (t !== v) return;
				m.value = e instanceof Error ? e.message : "Logs could not be loaded.", p.value = `Live tail error: ${m.value}`;
			} finally {
				t === v && (o.value = !1);
			}
		}
		async function A() {
			D(), a.value = 0, i.value = [], await k(!0), O(1e3);
		}
		async function j() {
			s.value = !s.value, s.value ? (D(), p.value = `Paused · ${y.value.length} visible lines`) : (await k(!1), O(250));
		}
		function M() {
			c.value = !c.value, r("autoScrollChange", c.value), c.value && E();
		}
		function N() {
			i.value = [], p.value = "Visible log view cleared. Live tail will continue.";
		}
		async function P() {
			let e = y.value.map((e) => `${C(e.ts)} ${e.level.toUpperCase().padEnd(8)} ${e.logger} ${e.message || e.display}`).join("\n");
			if (!e.trim()) {
				p.value = "No visible logs to copy.";
				return;
			}
			try {
				await navigator.clipboard.writeText(e), p.value = `Copied ${y.value.length} visible log line${y.value.length === 1 ? "" : "s"}.`, r("notify", p.value, "success");
			} catch (e) {
				m.value = e instanceof Error ? e.message : "Clipboard access failed.", p.value = `Copy failed: ${m.value}`, r("notify", p.value, "error");
			}
		}
		return Er(async () => {
			g = !0, await k(!0), O(1e3);
		}), kr(() => {
			g = !1, v += 1, D();
		}), (e, t) => (q(), J("section", XS, [Y("section", ZS, [
			Y("header", QS, [t[3] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Live application stream"),
				Y("h2", null, "Runtime console"),
				Y("p", null, "Backend messages, warnings, errors, and Core or Portal activity update automatically.")
			], -1), Y("span", { class: B(["tv-live-pill", { warning: s.value || !!m.value }]) }, [t[2] ||= Y("i", null, null, -1), X(V(m.value ? "Connection issue" : s.value ? "Paused" : "Live"), 1)], 2)]),
			Y("div", $S, [
				Y("div", null, [t[4] ||= Y("span", null, "Visible", -1), Y("strong", null, V(y.value.length), 1)]),
				Y("div", null, [t[5] ||= Y("span", null, "Warnings", -1), Y("strong", null, V(b.value), 1)]),
				Y("div", null, [t[6] ||= Y("span", null, "Errors", -1), Y("strong", null, V(x.value), 1)]),
				Y("div", null, [t[7] ||= Y("span", null, "Cursor", -1), Y("strong", null, V(a.value), 1)])
			]),
			Y("div", eC, [
				Y("button", {
					class: "tv-button",
					type: "button",
					disabled: o.value,
					onClick: A
				}, V(o.value ? "Refreshing…" : "Refresh"), 9, tC),
				Y("button", {
					class: B(["tv-button", { active: s.value }]),
					type: "button",
					onClick: j
				}, V(s.value ? "Resume" : "Pause"), 3),
				Y("button", {
					class: B(["tv-button", { active: c.value }]),
					type: "button",
					onClick: M
				}, "Auto-scroll " + V(c.value ? "on" : "off"), 3),
				Y("button", {
					class: "tv-button",
					type: "button",
					onClick: P
				}, "Copy visible"),
				Y("button", {
					class: "tv-button danger",
					type: "button",
					onClick: N
				}, "Clear view")
			]),
			Y("div", nC, [
				Y("label", null, [t[9] ||= X("Level ", -1), W(Y("select", {
					"onUpdate:modelValue": t[0] ||= (e) => l.value = e,
					onChange: A
				}, [...t[8] ||= [ba("<option value=\"\">All levels</option><option value=\"info\">Info+</option><option value=\"warning\">Warning+</option><option value=\"error\">Error+</option><option value=\"critical\">Critical</option>", 5)]], 544), [[ds, l.value]])]),
				Y("label", null, [
					t[10] ||= X("Logger ", -1),
					W(Y("input", {
						"onUpdate:modelValue": t[1] ||= (e) => u.value = e,
						type: "text",
						list: "tlogs-logger-options",
						placeholder: "Filter logger name",
						onChange: A,
						onKeydown: Ss(bs(A, ["prevent"]), ["enter"])
					}, null, 40, rC), [[$, u.value]]),
					Y("datalist", iC, [(q(!0), J(K, null, G(d.value, (e) => (q(), J("option", {
						key: e,
						value: e
					}, null, 8, aC))), 128))])
				]),
				Y("span", {
					class: B(["tlogs-status", { error: !!m.value }]),
					"aria-live": "polite"
				}, V(p.value), 3)
			]),
			Y("div", {
				ref_key: "logElement",
				ref: h,
				class: "app-log-events tlogs-events",
				role: "log",
				"aria-live": "polite"
			}, [(q(!0), J(K, null, G(y.value, (e) => (q(), J("article", {
				key: e.seq,
				class: B(["app-log-line", e.level])
			}, [
				Y("span", oC, V(C(e.ts)), 1),
				Y("span", sC, V(e.level.toUpperCase()), 1),
				Y("div", cC, [
					Y("strong", null, V(e.logger), 1),
					w(e) ? (q(), J("small", lC, V(w(e)), 1)) : Z("", !0),
					Y("pre", null, V(e.display || e.message), 1),
					e.exception ? (q(), J("pre", uC, V(e.exception), 1)) : Z("", !0)
				])
			], 2))), 128)), y.value.length ? Z("", !0) : (q(), J("div", dC, V(o.value ? "Loading application logs…" : "No logs match these filters yet."), 1))], 512)
		])]));
	}
}), pC = { class: "tset-resource" }, mC = { class: "tset-resource-grid" }, hC = { class: "tv-panel tset-form-card" }, gC = { class: "tv-form-grid" }, _C = ["value"], vC = { class: "tv-panel tset-form-card" }, yC = { class: "tv-form-grid" }, bC = { class: "tv-toggle" }, xC = { class: "tv-toggle" }, SC = { class: "tset-save-bar" }, CC = ["disabled"], wC = {
	class: "tv-modal popup-effect-preview-dialog",
	role: "dialog",
	"aria-modal": "true",
	"aria-label": "Compotato popup effect preview"
}, TC = /* @__PURE__ */ sr({
	__name: "MiscSettings",
	props: {
		settings: {},
		endpoint: {}
	},
	emits: ["saved", "notify"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ Et({
			popup_effect_style: "flame",
			emoji_enable_on_reaction_add: !0,
			emoji_enable_auto_reaction_on_reply: !0,
			emoji_reaction_chain_chance_percent: 100,
			emoji_reply_reaction_chance_percent: 12,
			emoji_reaction_chain_cooldown_seconds: 30,
			emoji_reply_reaction_cooldown_seconds: 120,
			emoji_min_message_length: 4
		}), a = /* @__PURE__ */ U(!1), o = /* @__PURE__ */ U(!1), s = /* @__PURE__ */ U(""), c = /* @__PURE__ */ U(""), l = /* @__PURE__ */ U(!1), u = [
			["disabled", "Disabled"],
			["flame", "Flame"],
			["dust", "Crumble to dust"],
			["glitch", "Glitch out"],
			["portal", "Portal swirl shut"],
			["melt", "Melt downward"]
		];
		function d(e, t, n, r) {
			let i = Number(e);
			return Number.isFinite(i) ? Math.min(r, Math.max(n, Math.round(i))) : t;
		}
		function f(e) {
			Object.assign(i, {
				popup_effect_style: String(e.popup_effect_style || "flame"),
				emoji_enable_on_reaction_add: e.emoji_enable_on_reaction_add !== !1,
				emoji_enable_auto_reaction_on_reply: e.emoji_enable_auto_reaction_on_reply !== !1,
				emoji_reaction_chain_chance_percent: d(e.emoji_reaction_chain_chance_percent, 100, 0, 100),
				emoji_reply_reaction_chance_percent: d(e.emoji_reply_reaction_chance_percent, 12, 0, 100),
				emoji_reaction_chain_cooldown_seconds: d(e.emoji_reaction_chain_cooldown_seconds, 30, 0, 86400),
				emoji_reply_reaction_cooldown_seconds: d(e.emoji_reply_reaction_cooldown_seconds, 120, 0, 86400),
				emoji_min_message_length: d(e.emoji_min_message_length, 4, 0, 200)
			}), o.value = !1;
		}
		function p() {
			o.value = !0, s.value = "", c.value = "";
		}
		function m() {
			document.body.dataset.popupEffect = String(i.popup_effect_style || "flame"), l.value = !0;
		}
		function h() {
			l.value = !1, document.body.dataset.popupEffect = String(n.settings.popup_effect_style || "flame");
		}
		async function g() {
			a.value = !0, s.value = "", c.value = "";
			let e = {
				popup_effect_style: i.popup_effect_style,
				emoji_enable_on_reaction_add: !!i.emoji_enable_on_reaction_add,
				emoji_enable_auto_reaction_on_reply: !!i.emoji_enable_auto_reaction_on_reply,
				emoji_reaction_chain_chance_percent: d(i.emoji_reaction_chain_chance_percent, 100, 0, 100),
				emoji_reply_reaction_chance_percent: d(i.emoji_reply_reaction_chance_percent, 12, 0, 100),
				emoji_reaction_chain_cooldown_seconds: d(i.emoji_reaction_chain_cooldown_seconds, 30, 0, 86400),
				emoji_reply_reaction_cooldown_seconds: d(i.emoji_reply_reaction_cooldown_seconds, 120, 0, 86400),
				emoji_min_message_length: d(i.emoji_min_message_length, 4, 0, 200)
			};
			try {
				let t = await js(n.endpoint, e);
				r("saved", t), f(t), c.value = "Misc settings saved and synchronized.", r("notify", c.value, "success");
			} catch (e) {
				s.value = e instanceof Error ? e.message : "Misc settings could not be saved.", r("notify", s.value, "error");
			} finally {
				a.value = !1;
			}
		}
		return On(() => n.settings, (e) => {
			o.value || f(e || {});
		}, { immediate: !0 }), kr(() => {
			l.value && (document.body.dataset.popupEffect = String(n.settings.popup_effect_style || "flame"));
		}), (e, t) => (q(), J("section", pC, [
			c.value || s.value ? (q(), J("div", {
				key: 0,
				class: B(["tv-notice", { error: !!s.value }]),
				"aria-live": "polite"
			}, V(s.value || c.value), 3)) : Z("", !0),
			Y("div", mC, [Y("section", hC, [t[10] ||= Y("header", null, [
				Y("span", { class: "tv-eyebrow" }, "Motion"),
				Y("h2", null, "Compotato popup effects"),
				Y("p", null, "Choose the closing animation used by modals and toast messages.")
			], -1), Y("div", gC, [Y("label", null, [t[8] ||= X(" Popup animation style ", -1), W(Y("select", {
				"onUpdate:modelValue": t[0] ||= (e) => i.popup_effect_style = e,
				onChange: p
			}, [(q(), J(K, null, G(u, (e) => Y("option", {
				key: e[0],
				value: e[0]
			}, V(e[1]), 9, _C)), 64))], 544), [[ds, i.popup_effect_style]])]), Y("div", { class: "tset-preview-control" }, [t[9] ||= Y("span", null, "Reduced-motion preferences are respected automatically.", -1), Y("button", {
				class: "tv-button",
				type: "button",
				onClick: m
			}, "Preview selected effect")])])]), Y("section", vC, [t[18] ||= Y("header", null, [
				Y("span", { class: "tv-eyebrow" }, "Emoji"),
				Y("h2", null, "Reaction behavior"),
				Y("p", null, "Control Discord reaction chains and automatic reactions to replies.")
			], -1), Y("div", yC, [
				Y("label", bC, [W(Y("input", {
					"onUpdate:modelValue": t[1] ||= (e) => i.emoji_enable_on_reaction_add = e,
					class: "tv-checkbox",
					type: "checkbox",
					onChange: p
				}, null, 544), [[cs, i.emoji_enable_on_reaction_add]]), t[11] ||= Y("span", null, [Y("strong", null, "Reaction-chain mode"), Y("small", null, "React when another Discord reaction is added.")], -1)]),
				Y("label", xC, [W(Y("input", {
					"onUpdate:modelValue": t[2] ||= (e) => i.emoji_enable_auto_reaction_on_reply = e,
					class: "tv-checkbox",
					type: "checkbox",
					onChange: p
				}, null, 544), [[cs, i.emoji_enable_auto_reaction_on_reply]]), t[12] ||= Y("span", null, [Y("strong", null, "Automatic reply reactions"), Y("small", null, "Allow a reaction after replying.")], -1)]),
				Y("label", null, [t[13] ||= X(" Reaction-chain chance (%) ", -1), W(Y("input", {
					"onUpdate:modelValue": t[3] ||= (e) => i.emoji_reaction_chain_chance_percent = e,
					type: "number",
					min: "0",
					max: "100",
					onInput: p
				}, null, 544), [[
					$,
					i.emoji_reaction_chain_chance_percent,
					void 0,
					{ number: !0 }
				]])]),
				Y("label", null, [t[14] ||= X(" Reply reaction chance (%) ", -1), W(Y("input", {
					"onUpdate:modelValue": t[4] ||= (e) => i.emoji_reply_reaction_chance_percent = e,
					type: "number",
					min: "0",
					max: "100",
					onInput: p
				}, null, 544), [[
					$,
					i.emoji_reply_reaction_chance_percent,
					void 0,
					{ number: !0 }
				]])]),
				Y("label", null, [t[15] ||= X(" Reaction-chain cooldown (seconds) ", -1), W(Y("input", {
					"onUpdate:modelValue": t[5] ||= (e) => i.emoji_reaction_chain_cooldown_seconds = e,
					type: "number",
					min: "0",
					max: "86400",
					onInput: p
				}, null, 544), [[
					$,
					i.emoji_reaction_chain_cooldown_seconds,
					void 0,
					{ number: !0 }
				]])]),
				Y("label", null, [t[16] ||= X(" Reply reaction cooldown (seconds) ", -1), W(Y("input", {
					"onUpdate:modelValue": t[6] ||= (e) => i.emoji_reply_reaction_cooldown_seconds = e,
					type: "number",
					min: "0",
					max: "86400",
					onInput: p
				}, null, 544), [[
					$,
					i.emoji_reply_reaction_cooldown_seconds,
					void 0,
					{ number: !0 }
				]])]),
				Y("label", null, [t[17] ||= X(" Minimum message length ", -1), W(Y("input", {
					"onUpdate:modelValue": t[7] ||= (e) => i.emoji_min_message_length = e,
					type: "number",
					min: "0",
					max: "200",
					onInput: p
				}, null, 544), [[
					$,
					i.emoji_min_message_length,
					void 0,
					{ number: !0 }
				]])])
			])])]),
			Y("footer", SC, [Y("div", null, [Y("strong", null, V(o.value ? "Unsaved changes" : "Misc settings are synchronized"), 1), t[19] ||= Y("span", null, "The server response becomes the active UI state.", -1)]), Y("button", {
				class: "tv-button primary",
				type: "button",
				disabled: a.value || !o.value,
				onClick: g
			}, V(a.value ? "Saving…" : "Save misc settings"), 9, CC)]),
			ga(kl, {
				open: l.value,
				"backdrop-class": "tv-modal-backdrop tset-modal",
				onClose: h
			}, {
				default: Sn(() => [Y("section", wC, [Y("header", null, [Y("div", null, [
					t[20] ||= Y("span", { class: "tv-eyebrow" }, "Compotato preview", -1),
					Y("h2", null, V(i.popup_effect_style === "disabled" ? "Simple" : `${i.popup_effect_style[0].toUpperCase()}${i.popup_effect_style.slice(1)}`) + " popup effect", 1),
					t[21] ||= Y("p", null, "This is how dialogs enter and leave throughout the Tater WebUI.", -1)
				]), Y("button", {
					class: "tv-button",
					type: "button",
					onClick: h
				}, "Close preview")])])]),
				_: 1
			}, 8, ["open"])
		]));
	}
}), EC = { class: "tm-stack tm-hf-browser" }, DC = { key: 0 }, OC = { key: 1 }, kC = { key: 2 }, AC = { key: 3 }, jC = { class: "tm-hf-selection-actions" }, MC = {
	key: 0,
	class: "tm-hf-auto-pill"
}, NC = ["disabled"], PC = {
	key: 0,
	class: "tm-hf-waiting"
}, FC = { class: "tm-hf-queue-list" }, IC = ["onClick"], LC = {
	key: 1,
	class: "tm-hf-activity"
}, RC = {
	key: 0,
	class: "tm-hf-current-progress"
}, zC = ["value"], BC = ["open"], VC = { class: "tm-progress-list" }, HC = { class: "tm-progress-copy" }, UC = { class: "tm-download-state" }, WC = ["value"], GC = ["onClick"], KC = {
	key: 0,
	class: "tv-notice"
}, qC = {
	key: 1,
	class: "tv-notice error",
	"aria-live": "polite"
}, JC = { class: "tm-form-card tm-hf-guide" }, YC = { class: "tm-hf-filters" }, XC = { class: "tm-field" }, ZC = ["value"], QC = { class: "tm-field" }, $C = ["value"], ew = { class: "tm-field" }, tw = ["disabled"], nw = {
	class: "tm-hf-view-switch",
	role: "group",
	"aria-label": "Model list"
}, rw = ["aria-pressed", "onClick"], iw = { class: "tm-hf-results" }, aw = { class: "tm-hf-results-head" }, ow = {
	key: 0,
	class: "tm-hf-selected-pill"
}, sw = ["aria-busy"], cw = { class: "tm-model-card-head" }, lw = { class: "tm-model-badges" }, uw = { class: "tm-model-kind" }, dw = {
	key: 0,
	class: "tm-model-installed-badge"
}, fw = [
	"aria-pressed",
	"disabled",
	"onClick"
], pw = { "aria-hidden": "true" }, mw = ["onClick"], hw = {
	key: 0,
	class: "tm-model-pick-note"
}, gw = { class: "tm-model-meta" }, _w = { key: 0 }, vw = { key: 1 }, yw = { key: 2 }, bw = { key: 3 }, xw = { key: 4 }, Sw = { class: "tm-model-card-actions" }, Cw = ["onClick"], ww = ["href"], Tw = {
	key: 0,
	class: "tv-notice"
}, Ew = ["disabled"], Dw = { class: "tm-form-card" }, Ow = { class: "tm-installed-list" }, kw = ["onClick"], Aw = {
	key: 0,
	class: "tv-notice"
}, jw = {
	class: "tv-modal tm-hf-detail-modal",
	role: "dialog",
	"aria-modal": "true",
	"aria-labelledby": "tm-hf-detail-title"
}, Mw = { class: "tv-modal-header" }, Nw = { id: "tm-hf-detail-title" }, Pw = { key: 0 }, Fw = {
	key: 0,
	class: "tv-notice"
}, Iw = { class: "tm-hf-detail-summary" }, Lw = ["href"], Rw = {
	key: 0,
	class: "tm-hf-file-section"
}, zw = {
	key: 0,
	class: "tv-notice error"
}, Bw = {
	key: 1,
	class: "tm-hf-file-list"
}, Vw = [
	"aria-pressed",
	"disabled",
	"onClick"
], Hw = { "aria-hidden": "true" }, Uw = {
	key: 2,
	class: "tm-status-card"
}, Ww = {
	key: 1,
	class: "tm-hf-file-section"
}, Gw = { class: "tm-hf-repo-files" }, Kw = { key: 0 }, qw = { class: "tm-hf-detail-footer" }, Jw = { key: 0 }, Yw = { key: 1 }, Xw = ["disabled"], Zw = /* @__PURE__ */ sr({
	__name: "HuggingFaceModels",
	props: {
		localModels: {},
		endpoints: {}
	},
	emits: ["notify", "localModels"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ U("llama_cpp"), a = /* @__PURE__ */ U("picks"), o = /* @__PURE__ */ U("text-generation"), s = /* @__PURE__ */ U(""), c = /* @__PURE__ */ U(!1), l = /* @__PURE__ */ U(!1), u = /* @__PURE__ */ U(!1), d = /* @__PURE__ */ U(""), f = /* @__PURE__ */ U(""), p = /* @__PURE__ */ U([]), m = /* @__PURE__ */ U(null), h = /* @__PURE__ */ U("hf_transformers"), g = /* @__PURE__ */ U("text-generation"), _ = /* @__PURE__ */ U([]), v = /* @__PURE__ */ U(!1), y = /* @__PURE__ */ U(""), b = /* @__PURE__ */ U({}), x = /* @__PURE__ */ U([]), S = /* @__PURE__ */ U({}), C = null, w = null, T = 0, E = [
			{
				value: "hf_transformers",
				label: "Transformers",
				help: "Full Hugging Face repositories for broad compatibility."
			},
			{
				value: "llama_cpp",
				label: "llama.cpp / GGUF",
				help: "Quantized GGUF files. Usually the best fit for local Tater models."
			},
			{
				value: "mlx_lm",
				label: "MLX LM",
				help: "Apple Silicon-optimized repositories for MLX."
			}
		], D = [
			{
				value: "text-generation",
				label: "Text / chat"
			},
			{
				value: "image-text-to-text",
				label: "Vision / images"
			},
			{
				value: "audio-text-to-text",
				label: "Audio understanding"
			},
			{
				value: "video-text-to-text",
				label: "Video understanding"
			}
		], O = [
			{
				value: "picks",
				label: "Tater Picks"
			},
			{
				value: "trending",
				label: "Trending"
			},
			{
				value: "downloads",
				label: "Most downloaded"
			},
			{
				value: "new",
				label: "New"
			}
		], k = Q(() => Array.isArray(n.localModels?.models) ? n.localModels.models : []), A = Q(() => m.value && Array.isArray(m.value.files) ? m.value.files : []), j = Q(() => m.value?.model && typeof m.value.model == "object" ? m.value.model : {}), M = Q(() => A.value.filter((e) => String(e.name || e.path || "").toLowerCase().endsWith(".gguf") && !String(e.name || e.path || "").toLowerCase().includes("mmproj"))), N = Q(() => A.value.filter((e) => String(e.name || e.path || "").toLowerCase().includes("mmproj"))), P = Q(() => Array.isArray(S.value.items) ? S.value.items : []), F = Q(() => x.value.length), I = Q(() => P.value.filter((e) => ["downloaded", "loaded"].includes(String(e.status || "").toLowerCase())).length), ee = Q(() => P.value.find((e) => !!e.active) || P.value.find((e) => ![
			"downloaded",
			"loaded",
			"error",
			"cancelled",
			"canceled"
		].includes(String(e.status || "").toLowerCase()))), te = Q(() => x.value.filter((e) => e.provider === h.value && e.repoId === z(j.value))), ne = Q(() => te.value.length > 0), L = Q(() => h.value !== "llama_cpp" && pe(h.value, z(j.value)));
		function R(e = {}) {
			return new URLSearchParams({
				provider: i.value,
				view: a.value,
				query: s.value.trim(),
				task: o.value,
				limit: "24",
				...e
			}).toString();
		}
		function z(e) {
			return String(e.repo_id || e.id || e.model_id || "").trim();
		}
		function re(e) {
			return String(e.name || z(e) || "Unknown model");
		}
		function ie(e) {
			let t = String(e || "").trim().toLowerCase().replaceAll("-", "_").replaceAll(" ", "_");
			return [
				"llama",
				"llamacpp",
				"llama_cpp",
				"llama.cpp",
				"gguf"
			].includes(t) || t.includes("gguf") || t.includes("llama.cpp") ? "llama_cpp" : [
				"mlx",
				"mlx_lm",
				"mlxlm",
				"apple_mlx",
				"apple_silicon"
			].includes(t) || t.includes("mlx") ? "mlx_lm" : [
				"hf",
				"huggingface",
				"hugging_face",
				"transformers",
				"hf_transformers",
				"local_transformers"
			].includes(t) || t.includes("transformers") ? "hf_transformers" : t;
		}
		function ae(e) {
			return ie(e.provider || i.value || "hf_transformers");
		}
		function oe(e) {
			let t = ie(e);
			return E.find((e) => e.value === t)?.label || e;
		}
		function se(e, t) {
			return `${e}|${t}`;
		}
		function ce(e) {
			let t = ae(e), n = z(e);
			return x.value.some((e) => e.provider === t && e.repoId === n);
		}
		function le(e) {
			return String(e.provider || "").trim();
		}
		function ue(e) {
			return String(e.model || "").trim();
		}
		function de(e) {
			return String(e.repo_id || "").trim() || ue(e).split("::", 1)[0];
		}
		function fe(e) {
			let t = String(e.filename || "").trim();
			if (t) return t;
			let n = ue(e);
			return n.includes("::") ? n.slice(n.indexOf("::") + 2) : "";
		}
		function pe(e, t, n = "") {
			let r = e === "llama_cpp" && n ? `${t}::${n}` : t;
			return k.value.some((i) => le(i) === e ? ue(i) === r ? !0 : de(i) === t ? e !== "llama_cpp" || !!(n && fe(i) === n) : !1 : !1);
		}
		function me(e) {
			let t = ae(e), n = z(e);
			return k.value.filter((e) => le(e) === t && de(e) === n).length;
		}
		function he(e) {
			let t = me(e);
			return t ? ae(e) === "llama_cpp" ? `${t} file${t === 1 ? "" : "s"} installed` : "Installed" : "";
		}
		function ge(e) {
			return ae(e) === "llama_cpp" ? String(e.preferred_gguf || "") : "";
		}
		function _e(e) {
			let t = ae(e), n = z(e), r = ge(e);
			return !!(n && (t !== "llama_cpp" || r) && pe(t, n, r));
		}
		function ve(e) {
			return pe(h.value, z(j.value), String(e.name || e.path || ""));
		}
		function ye(e) {
			!e || pe(h.value, z(j.value), e) || (_.value = _.value.includes(e) ? _.value.filter((t) => t !== e) : [..._.value, e]);
		}
		function be(e) {
			let t = Math.max(0, Number(e) || 0);
			return t >= 1e6 ? `${(t / 1e6).toFixed(1)}M` : t >= 1e3 ? `${(t / 1e3).toFixed(1)}K` : Math.round(t).toLocaleString();
		}
		function xe(e) {
			let t = Math.max(0, Number(e) || 0);
			if (!t) return "";
			let n = [
				"B",
				"KB",
				"MB",
				"GB",
				"TB"
			], r = 0;
			for (; t >= 1024 && r < n.length - 1;) t /= 1024, r += 1;
			return `${t.toFixed(+(r > 1 && t < 10))} ${n[r]}`;
		}
		function H(e) {
			let t = Math.max(0, Math.round(Number(e) || 0));
			return t ? t < 60 ? `${t}s left` : t < 3600 ? `${Math.ceil(t / 60)}m left` : `${Math.floor(t / 3600)}h ${Math.ceil(t % 3600 / 60)}m left` : "";
		}
		function Se(e) {
			return D.find((t) => t.value === String(e || ""))?.label || "Model";
		}
		function Ce(e) {
			let t = e.split("/").filter(Boolean).map((e) => encodeURIComponent(e)).join("/");
			return t ? `https://huggingface.co/${t}` : "https://huggingface.co/models";
		}
		function we(e) {
			let t = [], n = xe(e.download_bytes || e.current_bytes), r = xe(e.download_total_bytes || e.current_total_bytes), i = xe(e.download_speed_bytes_per_sec || e.current_speed_bytes_per_sec), a = H(e.download_eta_seconds || e.current_eta_seconds);
			return n && r && t.push(`${n} of ${r}`), i && t.push(`${i}/s`), a && t.push(a), t.join(" · ") || String(e.message || e.status || "Waiting");
		}
		function Te(e) {
			let t = String(e.status || "pending").toLowerCase();
			return t === "downloaded" || t === "loaded" ? "Downloaded" : t === "error" ? "Failed" : t === "cancelled" || t === "canceled" ? "Cancelled" : t === "cancelling" ? "Cancelling" : t === "pending" ? "Waiting" : e.active ? "Downloading" : t.charAt(0).toUpperCase() + t.slice(1);
		}
		function Ee(e) {
			let t = String(e.status || "").toLowerCase();
			return ["downloaded", "loaded"].includes(t) ? "success" : t === "error" ? "error" : [
				"cancelled",
				"canceled",
				"cancelling"
			].includes(t) ? "cancelled" : e.active ? "active" : "pending";
		}
		async function De(e = !1) {
			let t = ++T;
			c.value = !0, f.value = "";
			try {
				let r = e && y.value ? { cursor: y.value } : {}, i = await As(`${n.endpoints.modelsHuggingFace}?${R(r)}`);
				if (t !== T) return;
				let a = Array.isArray(i.models) ? i.models : [];
				p.value = e ? [...p.value, ...a] : a, y.value = String(i.next_cursor || ""), b.value = i.integration && typeof i.integration == "object" ? i.integration : {};
			} catch (e) {
				t === T && (f.value = e instanceof Error ? e.message : "Hugging Face models could not be loaded.");
			} finally {
				t === T && (c.value = !1);
			}
		}
		async function Oe(e) {
			let t = z(e), r = ae(e);
			if (!t) throw Error("This model does not include a Hugging Face repository id.");
			return As(`${n.endpoints.modelsHuggingFaceDetail}?${new URLSearchParams({
				repo_id: t,
				provider: r
			})}`);
		}
		async function ke(e) {
			m.value = null, h.value = ae(e), g.value = o.value, _.value = [], v.value = !0, l.value = !0, f.value = "";
			try {
				m.value = await Oe(e);
				let t = z(e), n = x.value.filter((e) => e.provider === h.value && e.repoId === t && e.filename).map((e) => e.filename), r = String(m.value.preferred_gguf || "");
				_.value = n.length ? n : r && !pe(h.value, t, r) ? [r] : [];
			} catch (e) {
				f.value = e instanceof Error ? e.message : "Model details could not be loaded.", v.value = !1;
			} finally {
				l.value = !1;
			}
		}
		function Ae() {
			v.value = !1;
		}
		function je(e, t, n, r = "", i = null) {
			let a = z(e);
			if (!a) return;
			let o = ie(t), s = o === "llama_cpp" && r ? `${a}::${r}` : a, c = {
				key: se(o, s),
				provider: o,
				providerLabel: oe(o),
				repoId: a,
				modelId: s,
				filename: r,
				task: n,
				sizeLabel: String(e.size_label || e.model_size || i?.model_size || "")
			}, l = x.value.findIndex((e) => e.key === c.key);
			l >= 0 ? x.value.splice(l, 1, c) : x.value.push(c), f.value = "", Fe();
		}
		function Me(e) {
			x.value = x.value.filter((t) => t.key !== e), !x.value.length && w !== null && (window.clearTimeout(w), w = null);
		}
		function Ne(e, t) {
			x.value = x.value.filter((n) => n.provider !== e || n.repoId !== t);
		}
		function Pe() {
			x.value = [], w !== null && window.clearTimeout(w), w = null;
		}
		function Fe(e = 180) {
			w !== null && window.clearTimeout(w), w = window.setTimeout(() => {
				w = null, Re();
			}, e);
		}
		async function Ie(e) {
			let t = ae(e), n = z(e);
			if (ce(e)) {
				Ne(t, n);
				return;
			}
			if (_e(e)) {
				await ke(e);
				return;
			}
			if (t !== "llama_cpp") {
				je(e, t, o.value);
				return;
			}
			let r = String(e.preferred_gguf || "");
			if (r) {
				je(e, t, o.value, r);
				return;
			}
			d.value = n, f.value = "";
			try {
				let r = await Oe(e), i = r.model && typeof r.model == "object" ? r.model : e, a = String(r.preferred_gguf || "");
				if (!a || pe(t, n, a)) {
					m.value = r, h.value = t, g.value = o.value, _.value = [], v.value = !0;
					return;
				}
				je(i, t, o.value, a, r);
			} catch (e) {
				f.value = e instanceof Error ? e.message : "The model could not be selected.";
			} finally {
				d.value = "";
			}
		}
		function Le() {
			if (!m.value) return;
			let e = z(j.value);
			if (e) {
				if (h.value === "llama_cpp" && !_.value.length) {
					f.value = "Choose at least one GGUF file before adding this model.";
					return;
				}
				Ne(h.value, e), h.value === "llama_cpp" ? _.value.forEach((t) => {
					pe(h.value, e, t) || je(j.value, h.value, g.value, t, m.value);
				}) : pe(h.value, e) || je(j.value, h.value, g.value, "", m.value), Ae();
			}
		}
		async function Re() {
			if (!(!x.value.length || u.value || S.value.running)) {
				u.value = !0, f.value = "";
				try {
					let e = x.value.slice(0, 32), t = new Set(e.map((e) => e.key)), i = await js(n.endpoints.modelsHuggingFaceDownload, { items: e.map((e) => ({
						provider: ie(e.provider),
						repo_id: e.repoId,
						model_id: e.modelId,
						filename: e.filename,
						task: e.task
					})) });
					if (i.already_running) {
						S.value = i, He();
						return;
					}
					if (!i.started) throw Error("The download batch could not be started.");
					S.value = i, x.value = x.value.filter((e) => !t.has(e.key));
					let a = Number(i.queued_count || e.length);
					r("notify", `${a} model file${a === 1 ? "" : "s"} started.`, "success"), He();
				} catch (e) {
					f.value = e instanceof Error ? e.message : "The model downloads could not start.", r("notify", f.value, "error");
				} finally {
					u.value = !1;
				}
			}
		}
		async function ze() {
			try {
				r("localModels", await As(n.endpoints.modelsLocalLlm));
			} catch {}
		}
		async function Be(e) {
			let t = String(e.model || "this model");
			if (window.confirm(`Delete ${t} from local model storage?`)) try {
				let i = await js(n.endpoints.modelsLocalLlmDelete, {
					provider: e.provider,
					model: e.model
				});
				i.local_llm_models && typeof i.local_llm_models == "object" ? r("localModels", i.local_llm_models) : await ze(), r("notify", `${t} deleted.`, "success");
			} catch (e) {
				f.value = e instanceof Error ? e.message : "Local model could not be deleted.";
			}
		}
		async function Ve() {
			try {
				S.value = await As(n.endpoints.modelsHfWarmup), S.value.running ? He() : (await ze(), C = null, x.value.length && Fe());
			} catch {
				C = null;
			}
		}
		function He() {
			C !== null && window.clearTimeout(C), C = window.setTimeout(Ve, 1200);
		}
		async function Ue(e) {
			try {
				S.value = await js(n.endpoints.modelsHfWarmupCancel, e ? { key: e.key } : { cancel_all: !0 }), S.value.running && He();
			} catch (e) {
				f.value = e instanceof Error ? e.message : "The download could not be canceled.";
			}
		}
		return Er(async () => {
			await Promise.all([De(), Ve()]);
		}), kr(() => {
			C !== null && window.clearTimeout(C), w !== null && window.clearTimeout(w);
		}), (e, t) => (q(), J("section", EC, [
			Y("article", { class: B(["tm-form-card tm-hf-download-center", { quiet: !F.value && !P.value.length }]) }, [
				Y("header", null, [Y("div", null, [
					t[8] ||= Y("span", { class: "tv-eyebrow" }, "Download center", -1),
					Y("h3", null, V(S.value.running ? `Downloading ${Math.min(I.value + 1, P.value.length)} of ${P.value.length}` : u.value ? `Starting ${F.value} model file${F.value === 1 ? "" : "s"}…` : F.value ? `${F.value} model file${F.value === 1 ? "" : "s"} waiting` : P.value.length ? "Latest download batch" : "Ready for model downloads"), 1),
					ee.value ? (q(), J("p", DC, [X(V(ee.value.model) + " · " + V(we(ee.value)), 1), F.value ? (q(), J(K, { key: 0 }, [X(" · " + V(F.value) + " queued next", 1)], 64)) : Z("", !0)])) : F.value ? (q(), J("p", OC, "Starting automatically. Files download one at a time.")) : P.value.length ? (q(), J("p", kC, V(I.value) + " of " + V(P.value.length) + " completed.", 1)) : (q(), J("p", AC, "Choose a model below and Tater will start it automatically."))
				]), Y("div", jC, [
					F.value ? (q(), J("span", MC, V(u.value ? "Starting…" : "Auto-start on"), 1)) : Z("", !0),
					F.value ? (q(), J("button", {
						key: 1,
						class: "tv-button",
						type: "button",
						onClick: Pe
					}, "Clear waiting")) : Z("", !0),
					f.value && F.value && !S.value.running ? (q(), J("button", {
						key: 2,
						class: "tv-button primary",
						type: "button",
						disabled: u.value,
						onClick: Re
					}, V(u.value ? "Retrying…" : "Retry queue"), 9, NC)) : Z("", !0),
					S.value.running ? (q(), J("button", {
						key: 3,
						class: "tv-button danger",
						type: "button",
						onClick: t[0] ||= (e) => Ue()
					}, "Cancel all")) : Z("", !0)
				])]),
				F.value ? (q(), J("section", PC, [Y("header", null, [t[9] ||= Y("div", null, [Y("strong", null, "Up next"), Y("span", null, "Starts automatically when the current file finishes.")], -1), Y("b", null, V(F.value) + " waiting", 1)]), Y("div", FC, [(q(!0), J(K, null, G(x.value, (e) => (q(), J("div", {
					key: e.key,
					class: "tm-hf-queue-item"
				}, [Y("div", null, [Y("strong", null, V(e.repoId), 1), Y("span", null, [
					X(V(e.providerLabel), 1),
					e.filename ? (q(), J(K, { key: 0 }, [X(" · " + V(e.filename), 1)], 64)) : Z("", !0),
					e.sizeLabel ? (q(), J(K, { key: 1 }, [X(" · " + V(e.sizeLabel), 1)], 64)) : Z("", !0)
				])]), Y("button", {
					type: "button",
					"aria-label": "Remove from download queue",
					onClick: (t) => Me(e.key)
				}, "×", 8, IC)]))), 128))])])) : Z("", !0),
				S.value.running || P.value.length ? (q(), J("section", LC, [S.value.running && ee.value ? (q(), J("div", RC, [Y("div", null, [
					t[10] ||= Y("span", null, "Current file", -1),
					Y("strong", null, V(Te(ee.value)), 1),
					Y("b", null, V(Math.round(Number(ee.value.progress || 0))) + "%", 1)
				]), Y("progress", {
					class: "tm-batch-progress",
					value: Number(ee.value.progress || 0),
					max: "100"
				}, null, 8, zC)])) : Z("", !0), Y("details", {
					class: "tm-download-details",
					open: !!S.value.running
				}, [Y("summary", null, [t[11] ||= Y("span", null, "Download activity", -1), Y("small", null, V(P.value.length) + " item" + V(P.value.length === 1 ? "" : "s") + " · " + V(I.value) + " complete", 1)]), Y("div", VC, [(q(!0), J(K, null, G(P.value, (e) => (q(), J("div", {
					key: String(e.key || e.model),
					class: B(["tm-progress-row", Ee(e)])
				}, [
					Y("div", HC, [Y("div", null, [Y("strong", null, V(e.model), 1), Y("span", UC, V(Te(e)), 1)]), Y("span", null, V(e.provider_label || e.provider) + " · " + V(we(e)), 1)]),
					Y("progress", {
						value: Number(e.progress || 0),
						max: "100"
					}, null, 8, WC),
					e.cancelable ? (q(), J("button", {
						key: 0,
						class: "tv-button",
						type: "button",
						onClick: (t) => Ue(e)
					}, "Cancel", 8, GC)) : Z("", !0)
				], 2))), 128))])], 8, BC)])) : Z("", !0)
			], 2),
			b.value.setup_needed ? (q(), J("div", KC, "Public models can still be available, but gated or private models need the Hugging Face integration enabled with an access token.")) : Z("", !0),
			f.value ? (q(), J("div", qC, V(f.value), 1)) : Z("", !0),
			Y("article", JC, [
				t[15] ||= ba("<header><div><span class=\"tv-eyebrow\">Hugging Face model library</span><h3>Choose models and Tater handles the queue</h3></div><ol class=\"tm-hf-steps\" aria-label=\"Download steps\"><li><span>1</span><strong>Runtime</strong></li><li><span>2</span><strong>Choose</strong></li><li><span>3</span><strong>Auto-download</strong></li></ol></header>", 1),
				Y("div", YC, [
					Y("label", XC, [t[12] ||= Y("span", { class: "tm-field-label" }, "Runtime", -1), W(Y("select", {
						"onUpdate:modelValue": t[1] ||= (e) => i.value = e,
						onChange: t[2] ||= (e) => De()
					}, [(q(), J(K, null, G(E, (e) => Y("option", {
						key: e.value,
						value: e.value
					}, V(e.label), 9, ZC)), 64))], 544), [[ds, i.value]])]),
					Y("label", QC, [t[13] ||= Y("span", { class: "tm-field-label" }, "Capability", -1), W(Y("select", {
						"onUpdate:modelValue": t[3] ||= (e) => o.value = e,
						onChange: t[4] ||= (e) => De()
					}, [(q(), J(K, null, G(D, (e) => Y("option", {
						key: e.value,
						value: e.value
					}, V(e.label), 9, $C)), 64))], 544), [[ds, o.value]])]),
					Y("form", {
						class: "tm-hf-search",
						onSubmit: t[6] ||= bs((e) => De(), ["prevent"])
					}, [Y("label", ew, [t[14] ||= Y("span", { class: "tm-field-label" }, "Search Hugging Face", -1), W(Y("input", {
						"onUpdate:modelValue": t[5] ||= (e) => s.value = e,
						type: "search",
						placeholder: "Model name or creator"
					}, null, 512), [[$, s.value]])]), Y("button", {
						class: "tv-button primary",
						type: "submit",
						disabled: c.value
					}, V(c.value ? "Searching…" : "Search"), 9, tw)], 32)
				]),
				Y("div", nw, [(q(), J(K, null, G(O, (e) => Y("button", {
					key: e.value,
					type: "button",
					class: B({ active: a.value === e.value }),
					"aria-pressed": a.value === e.value,
					onClick: (t) => {
						a.value = e.value, De();
					}
				}, V(e.label), 11, rw)), 64))])
			]),
			Y("section", iw, [
				Y("header", aw, [Y("div", null, [Y("h3", null, V(O.find((e) => e.value === a.value)?.label) + " models", 1), Y("p", null, V(c.value && !p.value.length ? "Finding compatible models…" : `${p.value.length} compatible ${Se(o.value).toLowerCase()} model${p.value.length === 1 ? "" : "s"} shown`), 1)]), F.value ? (q(), J("span", ow, V(F.value) + " selected", 1)) : Z("", !0)]),
				Y("div", {
					class: "tm-model-grid",
					"aria-busy": c.value
				}, [(q(!0), J(K, null, G(p.value, (e) => (q(), J("article", {
					key: String(e.repo_id || e.id),
					class: B(["tm-model-card", {
						selected: ce(e),
						pick: e.tater_pick,
						installed: me(e) > 0,
						"target-installed": _e(e)
					}])
				}, [
					Y("div", cw, [Y("div", lw, [Y("span", uw, V(e.tater_pick_label || e.pipeline_tag || Se(o.value)), 1), he(e) ? (q(), J("span", dw, "✓ " + V(he(e)), 1)) : Z("", !0)]), Y("button", {
						class: B(["tm-model-select", {
							selected: ce(e),
							installed: _e(e)
						}]),
						type: "button",
						"aria-pressed": ce(e),
						disabled: !!d.value || _e(e),
						onClick: (t) => Ie(e)
					}, [Y("span", pw, V(_e(e) || ce(e) ? "✓" : "↓"), 1), X(V(d.value === z(e) ? "Checking…" : _e(e) ? "Installed" : ce(e) ? "Queued" : "Download"), 1)], 10, fw)]),
					Y("button", {
						class: "tm-model-card-preview",
						type: "button",
						onClick: (t) => ke(e)
					}, [
						Y("strong", null, V(re(e)), 1),
						Y("small", null, V(e.author || z(e).split("/")[0]) + " · " + V(oe(ae(e))), 1),
						e.tater_pick_note ? (q(), J("span", hw, V(e.tater_pick_note), 1)) : Z("", !0),
						Y("div", gw, [
							e.size_label || e.model_size ? (q(), J("span", _w, V(e.size_label || e.model_size), 1)) : Z("", !0),
							e.supports_vision ? (q(), J("span", vw, "Vision")) : Z("", !0),
							e.supports_audio ? (q(), J("span", yw, "Audio")) : Z("", !0),
							e.supports_video ? (q(), J("span", bw, "Video")) : Z("", !0),
							Y("span", null, V(be(e.downloads)) + " downloads", 1),
							e.likes ? (q(), J("span", xw, V(be(e.likes)) + " likes", 1)) : Z("", !0)
						])
					], 8, mw),
					Y("div", Sw, [Y("button", {
						class: "tv-button",
						type: "button",
						onClick: (t) => ke(e)
					}, "Details & files", 8, Cw), Y("a", {
						class: "tv-button",
						href: Ce(z(e)),
						target: "_blank",
						rel: "noopener noreferrer"
					}, "Open Hub", 8, ww)])
				], 2))), 128)), !c.value && !p.value.length ? (q(), J("div", Tw, "No compatible models matched this search. Try another capability, runtime, or search phrase.")) : Z("", !0)], 8, sw),
				y.value ? (q(), J("button", {
					key: 0,
					class: "tv-button tm-load-more",
					type: "button",
					disabled: c.value,
					onClick: t[7] ||= (e) => De(!0)
				}, V(c.value ? "Loading…" : "Load more models"), 9, Ew)) : Z("", !0)
			]),
			Y("article", Dw, [Y("header", null, [t[16] ||= Y("div", null, [Y("h3", null, "Installed local models"), Y("p", null, "These models are ready to select in Tater's LLM, Vision, Audio, and Video settings.")], -1), Y("button", {
				class: "tv-button",
				type: "button",
				onClick: ze
			}, "Refresh")]), Y("div", Ow, [(q(!0), J(K, null, G(k.value, (e) => (q(), J("div", { key: `${e.provider}:${e.model}` }, [Y("div", null, [Y("strong", null, V(e.model), 1), Y("span", null, [
				X(V(e.provider_label || e.provider), 1),
				e.supports_vision ? (q(), J(K, { key: 0 }, [X(" · Vision")], 64)) : Z("", !0),
				e.supports_audio ? (q(), J(K, { key: 1 }, [X(" · Audio")], 64)) : Z("", !0),
				e.supports_video ? (q(), J(K, { key: 2 }, [X(" · Video")], 64)) : Z("", !0)
			])]), Y("button", {
				class: "tv-button danger",
				type: "button",
				onClick: (t) => Be(e)
			}, "Delete", 8, kw)]))), 128)), k.value.length ? Z("", !0) : (q(), J("div", Aw, "No local models are installed yet."))])]),
			ga(kl, {
				open: v.value,
				"backdrop-class": "tv-modal-backdrop tset-modal",
				onClose: Ae
			}, {
				default: Sn(() => [Y("section", jw, [Y("header", Mw, [Y("div", null, [
					t[17] ||= Y("span", { class: "tv-eyebrow" }, "Model details", -1),
					Y("h2", Nw, V(l.value ? "Loading model…" : re(j.value)), 1),
					l.value ? Z("", !0) : (q(), J("p", Pw, V(oe(h.value)) + " · " + V(Se(g.value)), 1))
				]), Y("button", {
					class: "tv-button",
					type: "button",
					onClick: Ae
				}, "Close")]), l.value ? (q(), J("div", Fw, "Reading model files from Hugging Face…")) : m.value ? (q(), J(K, { key: 1 }, [
					Y("div", Iw, [Y("p", null, V(j.value.description || "Review the available files and add this model to your download list."), 1), Y("a", {
						class: "tv-button",
						href: Ce(z(j.value)),
						target: "_blank",
						rel: "noopener noreferrer"
					}, "Open on Hugging Face", 8, Lw)]),
					h.value === "llama_cpp" ? (q(), J("section", Rw, [
						t[19] ||= Y("div", null, [Y("h3", null, "Choose one or more GGUF files"), Y("p", null, "Select every quantization you want, then add them to the download center together. Files already on this Tater are disabled.")], -1),
						M.value.length ? (q(), J("div", Bw, [(q(!0), J(K, null, G(M.value, (e) => (q(), J("button", {
							key: String(e.name || e.path),
							type: "button",
							class: B(["tm-hf-file-choice", {
								selected: _.value.includes(String(e.name || e.path)),
								installed: ve(e)
							}]),
							"aria-pressed": _.value.includes(String(e.name || e.path)),
							disabled: ve(e),
							onClick: (t) => ye(String(e.name || e.path))
						}, [
							Y("span", null, V(e.name || e.path), 1),
							Y("small", null, [
								X(V(e.quant || "GGUF"), 1),
								e.size_label ? (q(), J(K, { key: 0 }, [X(" · " + V(e.size_label), 1)], 64)) : Z("", !0),
								ve(e) ? (q(), J(K, { key: 1 }, [X(" · Already installed")], 64)) : Z("", !0)
							]),
							Y("i", Hw, V(ve(e) || _.value.includes(String(e.name || e.path)) ? "✓" : ""), 1)
						], 10, Vw))), 128))])) : (q(), J("div", zw, "No downloadable GGUF files were found in this repository.")),
						N.value.length ? (q(), J("div", Uw, [Y("div", null, [t[18] ||= Y("strong", null, "Vision projector included", -1), Y("span", null, V(m.value.preferred_mmproj || "Tater will match it automatically."), 1)])])) : Z("", !0)
					])) : (q(), J("section", Ww, [Y("div", null, [t[20] ||= Y("h3", null, "Repository download", -1), Y("p", null, V(h.value === "mlx_lm" ? "The full MLX repository, including tokenizer and config files, will be downloaded together." : "The full Transformers repository and its required files will be downloaded together."), 1)]), Y("div", Gw, [(q(!0), J(K, null, G(A.value.slice(0, 14), (e) => (q(), J("span", { key: String(e.name || e.path) }, V(e.name || e.path), 1))), 128)), A.value.length > 14 ? (q(), J("small", Kw, "+ " + V(A.value.length - 14) + " more files", 1)) : Z("", !0)])])),
					Y("footer", qw, [Y("div", null, [Y("strong", null, V(L.value ? "This model is already installed" : h.value === "llama_cpp" ? `${_.value.length} file${_.value.length === 1 ? "" : "s"} selected` : ne.value ? "Already queued" : "Ready to download"), 1), h.value === "llama_cpp" ? (q(), J("span", Jw, V(_.value.length ? "Selected files will download one at a time." : "Choose one or more available GGUF files above."), 1)) : (q(), J("span", Yw, V(L.value ? "Tater will not download it again." : "The complete repository will download automatically."), 1))]), Y("button", {
						class: "tv-button primary",
						type: "button",
						disabled: L.value || h.value === "llama_cpp" && !_.value.length,
						onClick: Le
					}, V(L.value ? "Installed" : ne.value ? "Update queue" : h.value === "llama_cpp" && _.value.length > 1 ? `Download ${_.value.length} files` : "Download model"), 9, Xw)])
				], 64)) : Z("", !0)])]),
				_: 1
			}, 8, ["open"])
		]));
	}
}), Qw = { class: "tm-stack tm-face-workspace" }, $w = { class: "tm-form-card tm-model-area-hero tm-face-hero" }, eT = { class: "tm-model-area-status" }, tT = { class: "tm-form-card tm-face-runtime-card" }, nT = { class: "tm-identity-master-toggle tm-face-master-toggle" }, rT = ["checked", "disabled"], iT = {
	key: 0,
	class: "tm-face-model-section"
}, aT = { class: "tm-speech-section-heading compact" }, oT = {
	class: "tm-choice-grid tm-face-model-grid",
	role: "group",
	"aria-label": "Face recognition model"
}, sT = ["disabled", "onClick"], cT = { key: 0 }, lT = {
	key: 1,
	class: "tm-media-route-summary"
}, uT = {
	key: 0,
	class: "tm-form-card tm-face-rebuild-card"
}, dT = { class: "tm-face-progress" }, fT = /* @__PURE__ */ sr({
	__name: "FaceModels",
	props: {
		settings: {},
		status: {},
		models: { default: () => [] },
		busy: {
			type: Boolean,
			default: !1
		}
	},
	emits: ["dirty"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = Q(() => !!n.settings.enabled), a = Q(() => String(n.settings.model_id || "facenet512")), o = Q(() => n.status.model_switch && typeof n.status.model_switch == "object" ? n.status.model_switch : {}), s = Q(() => String(o.value.state || "")), c = Q(() => [
			"queued",
			"embedding",
			"rebuilding"
		].includes(s.value)), l = Q(() => Math.max(0, Math.min(100, Number(o.value.progress || n.status.progress || 0)))), u = Q(() => {
			let e = String(n.status.state || (i.value ? "idle" : "disabled"));
			return c.value ? "Rebuilding face embeddings" : e === "loading" ? "Loading model" : e === "error" ? "Runtime error" : e === "disabled" ? "Disabled" : "Ready";
		}), d = Q(() => String(n.status.accelerator || n.status.accelerator_target || "CPU")), f = Q(() => n.status.identity_count ?? n.status.face_count ?? "—"), p = {
			facenet512: {
				mark: "FN",
				short: "Reliable default with broad compatibility and strong local recognition."
			},
			adaface_ir50_webface4m: {
				mark: "ADA",
				short: "Experimental newer embedding model for varied faces and lighting."
			}
		};
		function m(e) {
			n.settings.model_id = e, r("dirty");
		}
		function h(e) {
			n.settings.enabled = e.target.checked, r("dirty");
		}
		return (t, n) => (q(), J("section", Qw, [
			Y("article", $w, [
				n[0] ||= Y("div", { class: "tm-face-scan-mark" }, [
					Y("i"),
					Y("i"),
					Y("i"),
					Y("i"),
					Y("span", null, "☺")
				], -1),
				n[1] ||= Y("div", { class: "tm-model-area-hero-copy" }, [
					Y("span", { class: "tv-eyebrow" }, "Visual identity"),
					Y("h3", null, "Recognize familiar faces locally"),
					Y("p", null, "Face images and embeddings stay in Tater data. Awareness can attach a known identity without sending the camera image to a recognition service.")
				], -1),
				Y("div", eT, [
					Y("span", null, [Y("i", { class: B({ ready: i.value && u.value !== "Runtime error" }) }, null, 2), X(V(u.value), 1)]),
					Y("span", null, V(d.value), 1),
					Y("span", null, V(f.value) + " known", 1)
				])
			]),
			Y("article", tT, [
				Y("header", null, [n[2] ||= Y("div", null, [
					Y("span", { class: "tv-eyebrow" }, "Runtime"),
					Y("h3", null, "Face recognition"),
					Y("p", null, "Enable recognition, then choose the embedding model used for every saved person.")
				], -1), Y("span", { class: B(["tm-speech-status-chip", { ready: i.value }]) }, V(i.value ? "Enabled" : "Off"), 3)]),
				Y("label", nT, [n[3] ||= Y("span", null, [Y("i", null, "☺"), Y("span", null, [Y("strong", null, "Enable Face ID"), Y("small", null, "Load the model and recognize faces from Awareness camera bursts.")])], -1), Y("input", {
					type: "checkbox",
					checked: i.value,
					disabled: e.busy,
					onChange: h
				}, null, 40, rT)]),
				i.value ? (q(), J("section", iT, [Y("div", aT, [n[4] ||= Y("div", null, [Y("h3", null, "Recognition model"), Y("p", null, "Changing models rebuilds every saved face embedding in the background.")], -1), Y("span", null, V(e.status.model || a.value), 1)]), Y("div", oT, [(q(!0), J(K, null, G(e.models, (t) => (q(), J("button", {
					key: String(t.id),
					type: "button",
					class: B({ active: a.value === String(t.id) }),
					disabled: e.busy,
					onClick: (e) => m(String(t.id))
				}, [
					Y("i", null, V(p[String(t.id)]?.mark || "FACE"), 1),
					Y("span", null, [
						Y("strong", null, V(t.label || t.id), 1),
						Y("small", null, V(p[String(t.id)]?.short || "Local face embedding model."), 1),
						t.experimental ? (q(), J("em", cT, "Experimental")) : Z("", !0)
					]),
					n[5] ||= Y("b", null, "✓", -1)
				], 10, sT))), 128))])])) : (q(), J("div", lT, [...n[6] ||= [Y("i", null, "OFF", -1), Y("span", null, [Y("strong", null, "Recognition is disabled"), Y("small", null, "Existing people and face images remain saved and can be used again when enabled.")], -1)]]))
			]),
			c.value ? (q(), J("article", uT, [Y("header", null, [n[7] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Background task"),
				Y("h3", null, "Rebuilding face embeddings"),
				Y("p", null, "Tater is converting saved face images for the selected model. Recognition resumes automatically when this finishes.")
			], -1), Y("strong", null, V(Math.round(l.value)) + "%", 1)]), Y("div", dT, [Y("i", { style: R({ width: `${l.value}%` }) }, null, 4)])])) : Z("", !0),
			e.status.message || e.status.error ? (q(), J("article", {
				key: 1,
				class: B(["tm-status-card", { error: e.status.error }])
			}, [Y("div", null, [Y("strong", null, V(e.status.error ? "Face ID needs attention" : u.value), 1), Y("span", null, V(e.status.error || e.status.message), 1)])], 2)) : Z("", !0)
		]));
	}
}), pT = {
	key: 0,
	class: "tv-notice"
}, mT = {
	key: 1,
	class: "tv-notice error"
}, hT = { class: "tm-form-card tm-model-area-hero tm-identity-hero" }, gT = { class: "tm-identity-hero-mark" }, _T = { class: "tm-model-area-hero-copy" }, vT = { class: "tv-eyebrow" }, yT = { class: "tm-model-area-status" }, bT = {
	key: 2,
	class: "tm-metrics tm-identity-metrics"
}, xT = {
	key: 4,
	class: "tv-tabs tm-inner-tabs tm-identity-tabs",
	"aria-label": "Speaker ID sections"
}, ST = {
	key: 5,
	class: "tm-form-card tm-identity-runtime-card"
}, CT = { class: "tm-speech-status-chip" }, wT = { class: "tm-identity-master-toggle" }, TT = ["checked", "disabled"], ET = { class: "tm-identity-section" }, DT = { class: "tm-speech-section-heading compact" }, OT = { class: "tm-wake-toggle-card tm-identity-option-card" }, kT = ["checked", "disabled"], AT = {
	key: 0,
	class: "tm-field-grid"
}, jT = { class: "tm-field" }, MT = ["value", "disabled"], NT = { class: "tm-field" }, PT = ["value", "disabled"], FT = {
	key: 1,
	class: "tm-identity-warning"
}, IT = { class: "tm-identity-section" }, LT = { class: "tm-field-grid" }, RT = { class: "tm-field" }, zT = ["value", "disabled"], BT = { class: "tm-field" }, VT = ["value", "disabled"], HT = {
	key: 1,
	class: "tm-identity-section"
}, UT = { class: "tm-speech-section-heading compact" }, WT = { class: "tm-wake-feedback-grid" }, GT = { class: "tm-wake-toggle-card" }, KT = ["checked", "disabled"], qT = {
	key: 0,
	class: "tm-wake-toggle-card"
}, JT = ["checked", "disabled"], YT = { class: "tm-field-grid" }, XT = {
	key: 0,
	class: "tm-field"
}, ZT = ["value", "disabled"], QT = { class: "tm-field" }, $T = ["value", "disabled"], eE = {
	key: 2,
	class: "tm-media-route-summary"
}, tE = {
	key: 0,
	class: "tm-status-card live tm-identity-capture-live"
}, nE = ["disabled"], rE = { class: "tm-form-card tm-identity-add-card" }, iE = { class: "tm-field tm-field-wide" }, aE = { class: "tm-inline-actions" }, oE = ["disabled"], sE = { class: "tm-card-grid tm-identity-speaker-grid" }, cE = { class: "tm-speaker-heading" }, lE = { class: "tm-field-grid" }, uE = { class: "tm-field" }, dE = ["onUpdate:modelValue"], fE = { class: "tm-field" }, pE = ["onUpdate:modelValue"], mE = ["value"], hE = { class: "tm-inline-actions" }, gE = ["disabled", "onClick"], _E = ["disabled", "onClick"], vE = ["disabled", "onClick"], yE = {
	key: 0,
	class: "tm-identity-empty"
}, bE = {
	key: 7,
	class: "tm-form-card tm-emotion-result-card"
}, xE = { class: "tm-speech-status-chip" }, SE = { class: "tm-emotion-result" }, CE = { class: "tm-emotion-copy" }, wE = /* @__PURE__ */ sr({
	__name: "IdentityModels",
	props: {
		kind: {},
		runtimeEndpoint: {},
		actionEndpoint: {}
	},
	emits: [
		"notify",
		"dirty",
		"subtab"
	],
	setup(e, { expose: t, emit: n }) {
		let r = e, i = n, a = /* @__PURE__ */ U({}), o = /* @__PURE__ */ Et({}), s = /* @__PURE__ */ Et({}), c = /* @__PURE__ */ Et({ speaker_name: "" }), l = /* @__PURE__ */ U(!1), u = /* @__PURE__ */ U(""), d = /* @__PURE__ */ U(""), f = /* @__PURE__ */ U(!1), p = /* @__PURE__ */ U(!1), m = /* @__PURE__ */ U("people"), h = null, g = Q(() => r.kind === "speakerid"), _ = Q(() => Array.isArray(a.value.settings_sections) ? a.value.settings_sections : []), v = Q(() => Array.isArray(a.value.summary_metrics) ? a.value.summary_metrics : []), y = Q(() => Array.isArray(a.value.speakers) ? a.value.speakers : []), b = Q(() => {
			let e = a.value.pending;
			return !e || typeof e != "object" ? null : String(e.speaker_id || "").trim() ? e : null;
		}), x = Q(() => a.value.availability && typeof a.value.availability == "object" ? a.value.availability : {}), S = Q(() => a.value.last_result && typeof a.value.last_result == "object" ? a.value.last_result : {}), C = Q(() => Object.fromEntries(_.value.flatMap((e) => Array.isArray(e.fields) ? e.fields : []).map((e) => [String(e.key || ""), e]).filter(([e]) => e))), w = Q(() => g.value ? "VOICE_SPEAKER_ID_ENABLED" : "VOICE_EMOTION_ID_ENABLED"), T = Q(() => !!o[w.value]), E = Q(() => !!o.VOICE_SPEAKER_ID_BEST_MATCH), D = Q(() => !!o.VOICE_EMOTION_ID_PROMPT_HINT_ENABLED), O = Q(() => Math.max(0, Math.min(100, Math.round(Number(S.value.score || 0) * 100)))), k = Q(() => (g.value ? [
			"Enrolled Speakers",
			"Last Speaker",
			"Last Score",
			"Model"
		] : [
			"Prompt Hint",
			"Last Tone",
			"Last Score",
			"Model"
		]).map((e) => v.value.find((t) => String(t.label || "") === e)).filter(Boolean));
		function A(e) {
			return C.value[e] || {};
		}
		function j(e) {
			return !!(u.value || A(e).disabled || A(e).read_only || A(e).readonly);
		}
		function M(e, t) {
			L(e, t);
		}
		function N(e) {
			return e.target.value;
		}
		function P(e) {
			return e.target.checked;
		}
		function F(e) {
			m.value = e, i("subtab", e);
		}
		function I(e = !1) {
			f.value && !e || (Object.keys(o).forEach((e) => delete o[e]), _.value.forEach((e) => {
				(Array.isArray(e.fields) ? e.fields : []).forEach((e) => {
					let t = String(e.key || "").trim(), n = String(e.type || "").trim().toLowerCase();
					t && ![
						"table",
						"readonly",
						"section",
						"led_preview"
					].includes(n) && !e.disabled && !e.read_only && !e.readonly && (o[t] = e.value ?? e.default ?? (n !== "checkbox" && ""));
				});
			}));
		}
		function ee(e = !1) {
			if (p.value && !e) return;
			let t = /* @__PURE__ */ new Set();
			y.value.forEach((e) => {
				let n = String(e.speaker_id || "");
				n && (t.add(n), s[n] || (s[n] = {}), (Array.isArray(e.fields) ? e.fields : []).forEach((e) => {
					s[n][String(e.key || "")] = e.value ?? "";
				}));
			}), Object.keys(s).forEach((e) => {
				t.has(e) || delete s[e];
			});
		}
		function te(e, t = !1) {
			let n = e.payload && typeof e.payload == "object" ? e.payload : e, r = g.value ? n.speaker_id : n.emotion_id;
			a.value = r && typeof r == "object" ? r : n, I(t), g.value && ee(t);
		}
		async function ne(e = !1) {
			e || (l.value = !0), d.value = "";
			try {
				te(await As(`${r.runtimeEndpoint}?panel=${r.kind}`), !e);
			} catch (e) {
				d.value = e instanceof Error ? e.message : "Identity runtime could not be loaded.";
			} finally {
				l.value = !1;
			}
		}
		function L(e, t) {
			o[e] = t, f.value = !0, i("dirty");
		}
		async function z(e, t = {}, n = "Settings updated.") {
			if (u.value) throw Error("Another identity action is already running.");
			u.value = e, d.value = "";
			try {
				let o = await js(r.actionEndpoint, {
					action: e,
					payload: t
				}), s = g.value ? o.speaker_id : o.emotion_id;
				if (s && typeof s == "object") {
					a.value = s;
					let t = e.endsWith("_settings_save"), n = [
						"speaker_id_speaker_save",
						"speaker_id_speaker_create",
						"speaker_id_speaker_delete"
					].includes(e);
					t && (f.value = !1), n && (p.value = !1), I(t), g.value && ee(n);
				}
				let c = String(o.message || n);
				return i("notify", c, "success"), o;
			} catch (e) {
				throw d.value = e instanceof Error ? e.message : "Identity action failed.", i("notify", d.value, "error"), e;
			} finally {
				u.value = "";
			}
		}
		async function re() {
			try {
				return await z(g.value ? "speaker_id_settings_save" : "emotion_id_settings_save", { values: { ...o } }, `${g.value ? "Speaker" : "Emotion"} ID settings saved.`);
			} catch {
				return {
					ok: !1,
					error: d.value
				};
			}
		}
		async function ie() {
			if (!String(c.speaker_name || "").trim()) {
				d.value = "Enter a speaker name first.";
				return;
			}
			try {
				await z("speaker_id_speaker_create", { values: { speaker_name: c.speaker_name } }, "Speaker created."), c.speaker_name = "";
			} catch {}
		}
		async function ae(e) {
			let t = String(e.speaker_id || "");
			try {
				await z("speaker_id_speaker_save", {
					speaker_id: t,
					values: { ...s[t] || {} }
				}, "Speaker saved.");
			} catch {}
		}
		function oe() {
			p.value = !0;
		}
		async function se(e) {
			let t = String(e.speaker_id || "");
			try {
				await z("speaker_id_enrollment_arm", {
					speaker_id: t,
					values: { preferred_selector: s[t]?.preferred_selector || "" }
				}, "The next voice turn will be captured.");
			} catch {}
		}
		async function ce(e) {
			let t = String(e.name || "this speaker");
			if (window.confirm(`Delete ${t} and all saved voice samples?`)) try {
				await z("speaker_id_speaker_delete", { speaker_id: String(e.speaker_id || "") }, "Speaker deleted.");
			} catch {}
		}
		async function le() {
			try {
				await z("speaker_id_pending_cancel", {}, "Pending speaker capture canceled.");
			} catch {}
		}
		return Er(() => {
			g.value && i("subtab", m.value), ne(), h = window.setInterval(() => {
				!u.value && document.visibilityState === "visible" && ne(!0);
			}, 12e3);
		}), kr(() => {
			h !== null && window.clearInterval(h);
		}), t({
			apply: re,
			refresh: ne
		}), (e, t) => (q(), J("section", { class: B(["tm-stack tm-identity-workspace", g.value ? "tm-identity-speaker" : "tm-identity-emotion"]) }, [
			l.value ? (q(), J("div", pT, "Loading live " + V(g.value ? "Speaker" : "Emotion") + " ID settings…", 1)) : Z("", !0),
			d.value ? (q(), J("div", mT, V(d.value), 1)) : Z("", !0),
			Y("article", hT, [
				Y("div", gT, V(g.value ? "ID" : "♪"), 1),
				Y("div", _T, [
					Y("span", vT, V(g.value ? "Voice identity" : "Voice tone"), 1),
					Y("h3", null, V(g.value ? "Know who is speaking" : "Understand how it was said"), 1),
					Y("p", null, V(g.value ? "Match each voice turn against enrolled local profiles before Hydra responds." : "Classify vocal emotion and optionally add a subtle tone hint to Hydra's prompt."), 1)
				]),
				Y("div", yT, [Y("span", null, [Y("i", { class: B({ ready: T.value }) }, null, 2), X(V(T.value ? "Enabled" : "Disabled"), 1)]), Y("span", null, V(x.value.available === !1 ? "Needs attention" : "Runtime ready"), 1)])
			]),
			k.value.length ? (q(), J("div", bT, [(q(!0), J(K, null, G(k.value, (e) => (q(), J("article", { key: String(e.label) }, [Y("span", null, V(e.label), 1), Y("strong", null, V(e.value), 1)]))), 128))])) : Z("", !0),
			x.value.detail || x.value.error ? (q(), J("article", {
				key: 3,
				class: B(["tm-status-card", { error: x.value.error || x.value.available === !1 }])
			}, [Y("strong", null, V(x.value.available === !1 ? "Runtime needs attention" : "Runtime ready"), 1), Y("span", null, V(x.value.detail || x.value.error), 1)], 2)) : Z("", !0),
			g.value ? (q(), J("nav", xT, [Y("button", {
				type: "button",
				class: B({ active: m.value === "people" }),
				onClick: t[0] ||= (e) => F("people")
			}, "People", 2), Y("button", {
				type: "button",
				class: B({ active: m.value === "settings" }),
				onClick: t[1] ||= (e) => F("settings")
			}, "Settings", 2)])) : Z("", !0),
			!g.value || m.value === "settings" ? (q(), J("article", ST, [
				Y("header", null, [Y("div", null, [
					t[13] ||= Y("span", { class: "tv-eyebrow" }, "Runtime", -1),
					Y("h3", null, V(g.value ? "Speaker matching" : "Emotion classification"), 1),
					Y("p", null, V(g.value ? "Choose how strict matching should be. Enrollment controls remain with each person below." : "Choose when tone context is useful and how confident the model must be."), 1)
				]), Y("span", CT, V(T.value ? "Active" : "Off"), 1)]),
				Y("label", wT, [Y("span", null, [Y("i", null, V(g.value ? "ID" : "♪"), 1), Y("span", null, [Y("strong", null, "Enable " + V(g.value ? "Speaker" : "Emotion") + " ID", 1), Y("small", null, V(A(w.value).description), 1)])]), Y("input", {
					type: "checkbox",
					checked: T.value,
					disabled: j(w.value),
					onChange: t[2] ||= (e) => M(w.value, P(e))
				}, null, 40, TT)]),
				T.value && g.value ? (q(), J(K, { key: 0 }, [Y("section", ET, [
					Y("div", DT, [t[14] ||= Y("div", null, [Y("h3", null, "Matching strategy"), Y("p", null, "Threshold mode is safer; Best Match is more aggressive with known households.")], -1), Y("span", null, V(E.value ? "Best match" : "Threshold"), 1)]),
					Y("label", OT, [Y("span", null, [t[15] ||= Y("strong", null, "Best Match Mode", -1), Y("small", null, V(A("VOICE_SPEAKER_ID_BEST_MATCH").description), 1)]), Y("input", {
						type: "checkbox",
						checked: E.value,
						disabled: j("VOICE_SPEAKER_ID_BEST_MATCH"),
						onChange: t[3] ||= (e) => M("VOICE_SPEAKER_ID_BEST_MATCH", P(e))
					}, null, 40, kT)]),
					E.value ? (q(), J("div", FT, [...t[18] ||= [Y("i", null, "!", -1), Y("span", null, [Y("strong", null, "Best Match always picks someone"), Y("small", null, "An unknown voice can be assigned to the closest enrolled speaker.")], -1)]])) : (q(), J("div", AT, [Y("label", jT, [
						t[16] ||= Y("span", { class: "tm-field-label" }, "Match threshold", -1),
						Y("input", {
							type: "number",
							value: o.VOICE_SPEAKER_ID_MATCH_THRESHOLD,
							min: "0",
							max: "1",
							step: "0.01",
							disabled: j("VOICE_SPEAKER_ID_MATCH_THRESHOLD"),
							onInput: t[4] ||= (e) => M("VOICE_SPEAKER_ID_MATCH_THRESHOLD", N(e))
						}, null, 40, MT),
						Y("small", null, V(A("VOICE_SPEAKER_ID_MATCH_THRESHOLD").description), 1)
					]), Y("label", NT, [
						t[17] ||= Y("span", { class: "tm-field-label" }, "Runner-up margin", -1),
						Y("input", {
							type: "number",
							value: o.VOICE_SPEAKER_ID_MATCH_MARGIN,
							min: "0",
							max: "1",
							step: "0.01",
							disabled: j("VOICE_SPEAKER_ID_MATCH_MARGIN"),
							onInput: t[5] ||= (e) => M("VOICE_SPEAKER_ID_MATCH_MARGIN", N(e))
						}, null, 40, PT),
						Y("small", null, V(A("VOICE_SPEAKER_ID_MATCH_MARGIN").description), 1)
					])]))
				]), Y("section", IT, [t[21] ||= Y("div", { class: "tm-speech-section-heading compact" }, [Y("div", null, [Y("h3", null, "Voice length"), Y("p", null, "Skip clips that are too short for a reliable embedding.")]), Y("span", null, "Seconds")], -1), Y("div", LT, [Y("label", RT, [
					t[19] ||= Y("span", { class: "tm-field-label" }, "Minimum for matching", -1),
					Y("input", {
						type: "number",
						value: o.VOICE_SPEAKER_ID_MIN_SPEECH_S,
						min: "0.4",
						max: "15",
						step: "0.05",
						disabled: j("VOICE_SPEAKER_ID_MIN_SPEECH_S"),
						onInput: t[6] ||= (e) => M("VOICE_SPEAKER_ID_MIN_SPEECH_S", N(e))
					}, null, 40, zT),
					Y("small", null, V(A("VOICE_SPEAKER_ID_MIN_SPEECH_S").description), 1)
				]), Y("label", BT, [
					t[20] ||= Y("span", { class: "tm-field-label" }, "Minimum for enrollment", -1),
					Y("input", {
						type: "number",
						value: o.VOICE_SPEAKER_ID_ENROLL_MIN_SPEECH_S,
						min: "1",
						max: "20",
						step: "0.05",
						disabled: j("VOICE_SPEAKER_ID_ENROLL_MIN_SPEECH_S"),
						onInput: t[7] ||= (e) => M("VOICE_SPEAKER_ID_ENROLL_MIN_SPEECH_S", N(e))
					}, null, 40, VT),
					Y("small", null, V(A("VOICE_SPEAKER_ID_ENROLL_MIN_SPEECH_S").description), 1)
				])])])], 64)) : T.value ? (q(), J("section", HT, [
					Y("div", UT, [t[22] ||= Y("div", null, [Y("h3", null, "Prompt behavior"), Y("p", null, "Classification can run quietly or provide tone context to Hydra.")], -1), Y("span", null, V(D.value ? "Prompt hints on" : "Classify only"), 1)]),
					Y("div", WT, [Y("label", GT, [Y("span", null, [t[23] ||= Y("strong", null, "Add To Prompt", -1), Y("small", null, V(A("VOICE_EMOTION_ID_PROMPT_HINT_ENABLED").description), 1)]), Y("input", {
						type: "checkbox",
						checked: D.value,
						disabled: j("VOICE_EMOTION_ID_PROMPT_HINT_ENABLED"),
						onChange: t[8] ||= (e) => M("VOICE_EMOTION_ID_PROMPT_HINT_ENABLED", P(e))
					}, null, 40, KT)]), D.value ? (q(), J("label", qT, [Y("span", null, [t[24] ||= Y("strong", null, "Use Neutral Context", -1), Y("small", null, V(A("VOICE_EMOTION_ID_INCLUDE_NEUTRAL").description), 1)]), Y("input", {
						type: "checkbox",
						checked: !!o.VOICE_EMOTION_ID_INCLUDE_NEUTRAL,
						disabled: j("VOICE_EMOTION_ID_INCLUDE_NEUTRAL"),
						onChange: t[9] ||= (e) => M("VOICE_EMOTION_ID_INCLUDE_NEUTRAL", P(e))
					}, null, 40, JT)])) : Z("", !0)]),
					Y("div", YT, [D.value ? (q(), J("label", XT, [
						t[25] ||= Y("span", { class: "tm-field-label" }, "Prompt confidence threshold", -1),
						Y("input", {
							type: "number",
							value: o.VOICE_EMOTION_ID_CONFIDENCE_THRESHOLD,
							min: "0",
							max: "1",
							step: "0.01",
							disabled: j("VOICE_EMOTION_ID_CONFIDENCE_THRESHOLD"),
							onInput: t[10] ||= (e) => M("VOICE_EMOTION_ID_CONFIDENCE_THRESHOLD", N(e))
						}, null, 40, ZT),
						Y("small", null, V(A("VOICE_EMOTION_ID_CONFIDENCE_THRESHOLD").description), 1)
					])) : Z("", !0), Y("label", QT, [
						t[26] ||= Y("span", { class: "tm-field-label" }, "Minimum speech length", -1),
						Y("input", {
							type: "number",
							value: o.VOICE_EMOTION_ID_MIN_SPEECH_S,
							min: "0.4",
							max: "15",
							step: "0.05",
							disabled: j("VOICE_EMOTION_ID_MIN_SPEECH_S"),
							onInput: t[11] ||= (e) => M("VOICE_EMOTION_ID_MIN_SPEECH_S", N(e))
						}, null, 40, $T),
						Y("small", null, V(A("VOICE_EMOTION_ID_MIN_SPEECH_S").description), 1)
					])])
				])) : (q(), J("div", eE, [...t[27] ||= [Y("i", null, "OFF", -1), Y("span", null, [Y("strong", null, "Runtime disabled"), Y("small", null, "Turn it on to reveal its matching and tuning controls.")], -1)]]))
			])) : Z("", !0),
			g.value && m.value === "people" ? (q(), J(K, { key: 6 }, [
				b.value ? (q(), J("article", tE, [Y("div", null, [t[28] ||= Y("i", { class: "tm-identity-pulse" }, null, -1), Y("span", null, [Y("strong", null, "Listening for " + V(b.value.speaker_name || "speaker"), 1), Y("small", null, "Speak one clear sentence from " + V(b.value.selector_label || "a satellite") + ". The next complete voice turn becomes a sample.", 1)])]), Y("button", {
					class: "tv-button",
					type: "button",
					disabled: !!u.value,
					onClick: le
				}, "Cancel capture", 8, nE)])) : Z("", !0),
				Y("article", rE, [
					t[30] ||= Y("header", null, [Y("div", null, [
						Y("span", { class: "tv-eyebrow" }, "New profile"),
						Y("h3", null, "Add a speaker"),
						Y("p", null, "Create the person first, then capture one or more clear voice samples.")
					]), Y("i", { class: "tm-identity-add-mark" }, "+")], -1),
					Y("label", iE, [t[29] ||= Y("span", { class: "tm-field-label" }, "Speaker name", -1), W(Y("input", {
						"onUpdate:modelValue": t[12] ||= (e) => c.speaker_name = e,
						type: "text",
						placeholder: "Name"
					}, null, 512), [[$, c.speaker_name]])]),
					Y("div", aE, [Y("button", {
						class: "tv-button",
						type: "button",
						disabled: !!u.value,
						onClick: ie
					}, "Add speaker", 8, oE)])
				]),
				Y("section", sE, [(q(!0), J(K, null, G(y.value, (e) => (q(), J("article", {
					key: String(e.speaker_id),
					class: "tm-form-card tm-speaker-card"
				}, [
					Y("header", null, [Y("div", cE, [Y("i", null, V(String(e.name || "?").charAt(0).toUpperCase()), 1), Y("div", null, [Y("h3", null, V(e.name || "Speaker"), 1), Y("p", null, V(e.sample_count || 0) + " voice sample" + V(Number(e.sample_count || 0) === 1 ? "" : "s") + " · Updated " + V(e.updated_at || "—"), 1)])]), Y("span", { class: B(["tm-speech-status-chip", { ready: Number(e.sample_count || 0) > 0 }]) }, V(Number(e.sample_count || 0) > 0 ? "Ready" : "Needs sample"), 3)]),
					Y("div", lE, [Y("label", uE, [t[31] ||= Y("span", { class: "tm-field-label" }, "Speaker name", -1), W(Y("input", {
						"onUpdate:modelValue": (t) => s[String(e.speaker_id)].speaker_name = t,
						type: "text",
						onInput: oe
					}, null, 40, dE), [[$, s[String(e.speaker_id)].speaker_name]])]), Y("label", fE, [
						t[32] ||= Y("span", { class: "tm-field-label" }, "Capture from satellite", -1),
						W(Y("select", {
							"onUpdate:modelValue": (t) => s[String(e.speaker_id)].preferred_selector = t,
							onChange: oe
						}, [(q(!0), J(K, null, G(a.value.selector_options || [], (e) => (q(), J("option", {
							key: String(e.value),
							value: e.value
						}, V(e.label), 9, mE))), 128))], 40, pE), [[ds, s[String(e.speaker_id)].preferred_selector]]),
						t[33] ||= Y("small", null, "Used for the next voice sample. Leave on Any satellite to capture from whichever device hears the speaker.", -1)
					])]),
					Y("div", hE, [
						Y("button", {
							class: "tv-button primary",
							type: "button",
							disabled: !!u.value,
							onClick: (t) => se(e)
						}, "Capture voice sample", 8, gE),
						Y("button", {
							class: "tv-button",
							type: "button",
							disabled: !!u.value,
							onClick: (t) => ae(e)
						}, "Save profile", 8, _E),
						Y("button", {
							class: "tv-button danger",
							type: "button",
							disabled: !!u.value,
							onClick: (t) => ce(e)
						}, "Delete", 8, vE)
					])
				]))), 128)), y.value.length ? Z("", !0) : (q(), J("div", yE, [...t[34] ||= [
					Y("i", null, "ID", -1),
					Y("strong", null, "No speakers enrolled yet", -1),
					Y("span", null, "Add a profile above, then capture a clear sentence.", -1)
				]]))])
			], 64)) : Z("", !0),
			g.value ? Z("", !0) : (q(), J("article", bE, [Y("header", null, [t[35] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Live result"),
				Y("h3", null, "Latest voice tone"),
				Y("p", null, "Updates automatically after a long enough voice turn is classified.")
			], -1), Y("span", xE, V(S.value.updated_at || "Waiting"), 1)]), Y("div", SE, [Y("div", {
				class: "tm-emotion-score",
				style: R({ "--tm-score": `${O.value * 3.6}deg` })
			}, [Y("span", null, [Y("strong", null, V(O.value) + "%", 1), t[36] ||= Y("small", null, "confidence", -1)])], 4), Y("div", CE, [
				t[37] ||= Y("span", null, "Detected tone", -1),
				Y("strong", null, V(S.value.emotion || "No result yet"), 1),
				Y("p", null, V(S.value.prompt_hint || "No prompt hint was added for the latest turn."), 1)
			])])]))
		], 2));
	}
}), TE = { class: "tm-stack tm-llm-workspace" }, EE = {
	class: "tv-tabs tm-inner-tabs tm-llm-tabs",
	"aria-label": "LLM model areas"
}, DE = {
	key: 0,
	class: "tv-notice error"
}, OE = { class: "tv-eyebrow" }, kE = { class: "tm-inline-actions" }, AE = { class: "tm-llm-route-state" }, jE = ["onClick"], ME = { class: "tm-llm-provider-section" }, NE = { class: "tm-llm-section-title" }, PE = ["aria-label"], FE = ["aria-pressed", "onClick"], IE = { class: "tm-llm-connection-card" }, LE = {
	key: 0,
	class: "tm-llm-route-note"
}, RE = {
	key: 1,
	class: "tm-field-grid tm-llm-route-fields"
}, zE = {
	key: 0,
	class: "tm-field tm-field-wide"
}, BE = ["onUpdate:modelValue"], VE = {
	key: 1,
	class: "tm-field"
}, HE = ["onUpdate:modelValue"], UE = {
	key: 2,
	class: "tm-field"
}, WE = ["onUpdate:modelValue"], GE = {
	key: 3,
	class: "tm-field tm-field-wide"
}, KE = ["onUpdate:modelValue", "onChange"], qE = ["value"], JE = ["value"], YE = { key: 0 }, XE = {
	key: 4,
	class: "tm-field tm-field-wide"
}, ZE = ["onUpdate:modelValue"], QE = {
	key: 5,
	class: "tm-field"
}, $E = ["onUpdate:modelValue", "max"], eD = {
	key: 6,
	class: "tm-inline-field tm-llm-discover"
}, tD = ["disabled", "onClick"], nD = ["onUpdate:modelValue"], rD = ["value"], iD = {
	key: 0,
	class: "tm-form-card tm-llm-runtime-card"
}, aD = {
	key: 0,
	class: "tm-llm-runtime-picker"
}, oD = ["onClick"], sD = ["disabled"], cD = { class: "tm-llm-context-layout" }, lD = { class: "tm-llm-context-control" }, uD = [
	"min",
	"max",
	"value"
], dD = [
	"min",
	"max",
	"value"
], fD = { class: "tm-llm-context-fit" }, pD = {
	class: "tm-llm-context-meter",
	"aria-hidden": "true"
}, mD = { class: "tm-llm-context-facts" }, hD = { class: "tm-llm-settings-section" }, gD = { class: "tm-field-grid" }, _D = { class: "tm-field" }, vD = { class: "tm-field" }, yD = { class: "tm-field" }, bD = { class: "tm-field" }, xD = { class: "tm-llm-switch" }, SD = { class: "tm-llm-settings-section" }, CD = { class: "tm-field-grid" }, wD = { class: "tm-field" }, TD = { class: "tm-field" }, ED = { class: "tm-field" }, DD = { class: "tm-llm-switch-grid" }, OD = { class: "tm-llm-switch" }, kD = { class: "tm-llm-switch" }, AD = { class: "tm-llm-feature-toggle" }, jD = {
	key: 0,
	class: "tm-llm-speculative-settings"
}, MD = { class: "tm-field" }, ND = { class: "tm-field tm-llm-draft-model" }, PD = { value: "" }, FD = ["value"], ID = { key: 0 }, LD = {
	key: 1,
	class: "tm-llm-draft-match good"
}, RD = {
	key: 2,
	class: "tm-llm-draft-match warn"
}, zD = {
	key: 3,
	class: "tm-llm-draft-match warn"
}, BD = { class: "tm-field tm-llm-draft-tokens" }, VD = {
	key: 1,
	class: "tm-llm-feature-off"
}, HD = {
	key: 2,
	class: "tm-llm-settings-section"
}, UD = { class: "tm-field-grid" }, WD = { class: "tm-field" }, GD = { class: "tm-field" }, KD = ["value"], qD = { class: "tm-field" }, JD = { class: "tm-field" }, YD = { class: "tm-llm-switch-grid" }, XD = { class: "tm-llm-switch" }, ZD = { class: "tm-llm-switch" }, QD = {
	key: 1,
	class: "tm-form-card tm-llm-no-runtime"
}, $D = { class: "tm-form-card tm-llm-special-card" }, eO = { class: "tm-llm-mode-picker" }, tO = {
	key: 0,
	class: "tm-form-card tm-llm-special-card"
}, nO = {
	class: "tm-llm-provider-picker compact",
	role: "group",
	"aria-label": "Spudex provider"
}, rO = ["onClick"], iO = { class: "tm-field-grid" }, aO = {
	key: 0,
	class: "tm-field tm-field-wide"
}, oO = {
	key: 1,
	class: "tm-field tm-field-wide"
}, sO = ["value"], cO = ["value"], lO = {
	key: 2,
	class: "tm-field tm-field-wide"
}, uO = { class: "tm-form-card tm-llm-beast-hero" }, dO = { class: "tm-llm-feature-toggle" }, fO = { class: "tm-llm-section-chip" }, pO = ["aria-label"], mO = ["disabled", "onClick"], hO = { class: "tm-field-grid" }, gO = {
	key: 0,
	class: "tm-field tm-field-wide"
}, _O = ["onUpdate:modelValue"], vO = {
	key: 1,
	class: "tm-field"
}, yO = ["onUpdate:modelValue"], bO = {
	key: 2,
	class: "tm-field"
}, xO = ["onUpdate:modelValue"], SO = {
	key: 3,
	class: "tm-field tm-field-wide"
}, CO = ["onUpdate:modelValue"], wO = ["value"], TO = ["value"], EO = {
	key: 4,
	class: "tm-field tm-field-wide"
}, DO = ["onUpdate:modelValue"], OO = {
	key: 5,
	class: "tm-field"
}, kO = ["onUpdate:modelValue", "max"], AO = /* @__PURE__ */ sr({
	__name: "LlmModels",
	props: {
		draft: {},
		localModels: {},
		remoteModelsEndpoint: {},
		contextEstimateEndpoint: {}
	},
	emits: ["dirty"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ U("base"), a = /* @__PURE__ */ U(""), o = /* @__PURE__ */ Et({}), s = /* @__PURE__ */ U(""), c = /* @__PURE__ */ U(""), l = /* @__PURE__ */ U({}), u = /* @__PURE__ */ U(!1), d = /* @__PURE__ */ U(""), f = [
			{
				value: "openai_compatible",
				label: "OpenAI-Compatible",
				short: "Connect to an API",
				mark: "API",
				local: !1
			},
			{
				value: "hf_transformers",
				label: "Transformers",
				short: "Run a local HF model",
				mark: "HF",
				local: !0
			},
			{
				value: "llama_cpp",
				label: "llama.cpp",
				short: "Run a local GGUF",
				mark: "GG",
				local: !0
			},
			{
				value: "llama_cpp_remote",
				label: "Remote llama.cpp",
				short: "Connect to a server",
				mark: "↗",
				local: !1
			},
			{
				value: "mlx_lm",
				label: "MLX LM",
				short: "Optimized for Apple Silicon",
				mark: "MX",
				local: !0
			},
			{
				value: "spud_link",
				label: "Paired Spud Hub",
				short: "Route through your hub",
				mark: "SP",
				local: !1
			}
		], p = [
			{
				id: "astraeus",
				label: "Astraeus",
				description: "Planning and orchestration."
			},
			{
				id: "thanatos",
				label: "Thanatos",
				description: "Critique, safety, and verification."
			},
			{
				id: "hermes",
				label: "Hermes",
				description: "Final response composition."
			}
		], m = {
			hf_transformers: {
				key: "hydra_hf_transformers_context_tokens",
				min: 256,
				fallback: 4096
			},
			llama_cpp: {
				key: "hydra_llama_cpp_context_tokens",
				min: 256,
				fallback: 4096
			},
			mlx_lm: {
				key: "hydra_mlx_lm_context_tokens",
				min: 128,
				fallback: 4096
			}
		}, h = {
			"draft-mtp": {
				label: "Multi-Token Prediction (MTP)",
				tokens: 3,
				help: "Uses prediction heads from a sidecar GGUF, or embedded heads when the main model includes them.",
				requiresDraft: !1
			},
			"draft-dflash": {
				label: "DFlash",
				tokens: 15,
				help: "Uses llama.cpp's DFlash decoder and requires a matching DFlash draft GGUF.",
				requiresDraft: !0
			},
			"draft-dspark": {
				label: "DSpark",
				tokens: 7,
				help: "Uses llama.cpp's DSpark decoder and requires a matching DSpark draft GGUF.",
				requiresDraft: !0
			}
		}, g = Q(() => Array.isArray(n.localModels?.models) ? n.localModels.models : []), _ = Q(() => ((!Array.isArray(n.draft.hydra_base_servers) || !n.draft.hydra_base_servers.length) && (n.draft.hydra_base_servers = [{
			provider: n.draft.hydra_llm_provider || "openai_compatible",
			host: n.draft.hydra_llm_host || "",
			port: n.draft.hydra_llm_port || "",
			model: n.draft.hydra_llm_model || "",
			api_key: n.draft.hydra_llm_api_key || "",
			llama_cpp_slot: n.draft.hydra_llama_cpp_base_slot || ""
		}]), n.draft.hydra_base_servers)), v = Q(() => {
			let e = /* @__PURE__ */ new Set();
			return _.value.forEach((t) => {
				ae(t.provider) && e.add(re(t));
			}), ae(n.draft.spudex_llm_provider) && e.add(String(n.draft.spudex_llm_provider)), n.draft.hydra_beast_mode_enabled && p.forEach((t) => {
				let r = String(n.draft[le(t.id, "provider")] || "");
				ae(r) && e.add(r);
			}), f.filter((t) => t.local && e.has(t.value));
		}), y = Q(() => f.find((e) => e.value === a.value)), b = Q(() => m[a.value] || m.llama_cpp), x = Q(() => {
			let e = _.value.find((e) => re(e) === a.value && String(e.model || "").trim());
			if (e) return String(e.model || "").trim();
			if (String(n.draft.spudex_llm_provider || "") === a.value) return String(n.draft.spudex_llm_model || "").trim();
			for (let e of p) if (String(n.draft[le(e.id, "provider")] || "") === a.value) return String(n.draft[le(e.id, "model")] || "").trim();
			return "";
		}), S = Q(() => _e(a.value).find((e) => String(e.model || "") === x.value) || null), C = Q(() => Math.max(b.value.min, Number(S.value?.max_context_tokens || 262144))), w = Q(() => {
			let e = Number(n.draft[b.value.key] || b.value.fallback);
			return Math.max(b.value.min, Math.min(C.value, Number.isFinite(e) ? Math.round(e) : b.value.fallback));
		}), T = Q(() => l.value.chat_context_window && typeof l.value.chat_context_window == "object" ? l.value.chat_context_window : {}), E = Q(() => Math.max(0, Number(T.value.minimum_context_window || 0))), D = Q(() => Math.max(0, Number(T.value.recommended_context_window || 0))), O = Q(() => Math.max(0, Number(T.value.prompt_tokens || 0))), k = Q(() => Math.max(0, Number(T.value.completion_budget_tokens || 0))), A = Q(() => {
			let e = T.value.breakdown && typeof T.value.breakdown == "object" ? T.value.breakdown : {};
			return Math.max(0, Number(T.value.capability_context_reserve_tokens ?? e.capability_reserve_tokens ?? 0)) + Math.max(0, Number(T.value.burst_context_reserve_tokens ?? e.burst_reserve_tokens ?? 0));
		}), j = Q(() => D.value || O.value + k.value + A.value), M = Q(() => d.value || T.value.error ? "warn" : D.value ? w.value < Math.max(1, E.value) ? "danger" : w.value < D.value ? "warn" : "good" : "idle"), N = Q(() => u.value ? "Reading stats" : M.value === "danger" ? "Too low" : M.value === "warn" ? "Tight" : M.value === "good" ? "Ready" : "Waiting"), P = Q(() => w.value > 0 && j.value > 0 ? Math.max(4, Math.min(100, j.value / w.value * 100)) : 0), F = Q(() => C.value > 0 ? Math.max(2, Math.min(100, w.value / C.value * 100)) : 0), I = Q(() => h[String(n.draft.hydra_llama_cpp_speculative_method || "draft-mtp")] || h["draft-mtp"]), ee = Q(() => a.value !== "llama_cpp" || !x.value ? null : ue("llama_cpp").find((e) => String(e.model || "") === x.value) || { model: x.value }), te = Q(() => {
			let e = ee.value, t = de(n.draft.hydra_llama_cpp_speculative_method || "draft-mtp");
			return !e || !t ? [] : ue("llama_cpp").filter((n) => pe(n) === t && ge(n, e));
		}), ne = Q(() => {
			let e = String(n.draft.hydra_llama_cpp_mtp_draft_model || "");
			return !e || te.value.some((t) => String(t.model || "") === e);
		}), L = Q(() => {
			let e = ee.value;
			return e ? String(e.filename || e.model || "the selected base model").split("::").pop() || "the selected base model" : "";
		});
		On(() => v.value.map((e) => e.value).join("|"), () => {
			v.value.some((e) => e.value === a.value) || (a.value = v.value[0]?.value || "");
		}, { immediate: !0 });
		function z() {
			r("dirty");
		}
		function re(e) {
			return String(e.provider || "openai_compatible");
		}
		function ie(e) {
			return f.find((t) => t.value === String(e || "")) || f[0];
		}
		function ae(e) {
			return [
				"hf_transformers",
				"llama_cpp",
				"mlx_lm"
			].includes(String(e || ""));
		}
		function oe(e) {
			return String(e || "") === "llama_cpp_remote";
		}
		function se(e) {
			return ["openai_compatible", "llama_cpp_remote"].includes(String(e || ""));
		}
		function ce(e) {
			return ["llama_cpp", "llama_cpp_remote"].includes(String(e || ""));
		}
		function le(e, t) {
			return `hydra_llm_${e}_${t}`;
		}
		function ue(e) {
			return g.value.filter((t) => String(t.provider || "") === String(e || ""));
		}
		function de(e) {
			let t = String(e || "").trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
			return [
				"mtp",
				"draft-mtp",
				"multi-token-prediction",
				"multitoken-prediction"
			].includes(t) ? "draft-mtp" : [
				"dflash",
				"d-flash",
				"draft-dflash",
				"draft-d-flash"
			].includes(t) ? "draft-dflash" : [
				"dspark",
				"d-spark",
				"draft-dspark",
				"draft-d-spark"
			].includes(t) ? "draft-dspark" : "";
		}
		function fe(e) {
			let t = String(e.model || e.model_id || "");
			return String(e.filename || t.split("::").pop() || e.model_path || "");
		}
		function pe(e) {
			let t = de(e.speculative_method || e.speculative_draft_method || e.draft_method);
			if (t) return t;
			let n = fe(e).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
			return /(^|-)d-?flash(-|$)/.test(n) ? "draft-dflash" : /(^|-)d-?spark(-|$)/.test(n) ? "draft-dspark" : /(^|-)(mtp|multi-token-prediction)(-|$)/.test(n) ? "draft-mtp" : "";
		}
		function me(e) {
			if (e.is_speculative_draft === !0 || pe(e)) return !0;
			let t = fe(e).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
			return /(^|-)(draft|sidecar|speculator|speculative)(-|$)/.test(t);
		}
		function he(e) {
			let t = /* @__PURE__ */ new Set([
				"gguf",
				"mtp",
				"dflash",
				"dspark",
				"draft",
				"sidecar",
				"speculator",
				"speculative",
				"nothink",
				"think",
				"imatrix",
				"ud",
				"k",
				"m",
				"s",
				"l",
				"xs",
				"xxs"
			]);
			return fe(e).toLowerCase().replace(/\.gguf$/i, "").split(/[^a-z0-9]+/).filter((e) => e && !t.has(e) && !/^(?:q|iq)\d/.test(e) && !/^(?:f|fp|bf)\d+$/.test(e));
		}
		function ge(e, t) {
			let n = String(e.repo_id || "").trim().toLowerCase(), r = String(t.repo_id || "").trim().toLowerCase(), i = !!(n && r && n === r), a = he(e), o = new Set(he(t));
			if (!a.length || !o.size) return i;
			if (!o.has(a[0])) return !1;
			let s = a.filter((e) => o.has(e));
			return s.length >= Math.min(3, a.length) && s.length / a.length >= .75;
		}
		function _e(e) {
			let t = ue(e);
			return String(e || "") === "llama_cpp" ? t.filter((e) => !me(e)) : t;
		}
		function ve(e, t) {
			return ue(e).some((e) => String(e.model || "") === String(t || ""));
		}
		function ye() {
			let e = String(n.draft.hydra_llama_cpp_mtp_draft_model || "");
			e && !te.value.some((t) => String(t.model || "") === e) && (n.draft.hydra_llama_cpp_mtp_draft_model = "");
		}
		function be(e) {
			re(e) === "llama_cpp" && ye(), z();
		}
		function xe(e, t) {
			e.provider = t, ae(t) && (a.value = t), z();
		}
		function H(e, t) {
			n.draft[e] = t, ae(t) && (a.value = t), z();
		}
		function Se() {
			n.draft.hydra_base_servers.push({
				provider: "openai_compatible",
				host: "",
				port: "",
				model: "",
				api_key: "",
				llama_cpp_slot: ""
			}), z();
		}
		function Ce(e) {
			_.value.length <= 1 || (n.draft.hydra_base_servers.splice(e, 1), z());
		}
		function we(e) {
			n.draft.spudex_llm_provider = e === "base" ? "" : String(n.draft.spudex_llm_provider || "openai_compatible"), z();
		}
		function Te(e) {
			let t = e.target, r = Number(t.value || b.value.fallback);
			n.draft[b.value.key] = String(Math.max(b.value.min, Math.min(C.value, Number.isFinite(r) ? Math.round(r) : b.value.fallback))), z();
		}
		function Ee(e) {
			let t = String(e.target.value || "draft-mtp");
			n.draft.hydra_llama_cpp_speculative_method = t, n.draft.hydra_llama_cpp_mtp_draft_tokens = String(h[t]?.tokens || 3), ye(), z();
		}
		function De(e) {
			let t = Math.max(0, Number(e) || 0);
			return t ? t >= 1024 && t % 1024 == 0 ? `${Math.round(t / 1024)}k` : Math.round(t).toLocaleString() : "unknown";
		}
		function Oe() {
			return d.value || T.value.error ? String(d.value || T.value.error) : D.value ? w.value < E.value ? `Set at least ${De(E.value)}. ${De(D.value)} gives Tater safer working room.` : w.value < D.value ? `The current window is tight. Raise it toward ${De(D.value)} when memory allows.` : `The selected ${De(w.value)} window has room for the current prompt stack.` : "Tater is preparing a live estimate from the current prompt, tools, cores, and chat history.";
		}
		async function ke() {
			if (!(!n.contextEstimateEndpoint || u.value)) {
				u.value = !0, d.value = "";
				try {
					l.value = await As(n.contextEstimateEndpoint);
				} catch (e) {
					d.value = e instanceof Error ? e.message : "Context estimate could not be loaded.";
				} finally {
					u.value = !1;
				}
			}
		}
		async function Ae(e, t) {
			s.value = e, c.value = "";
			try {
				let r = await js(n.remoteModelsEndpoint, {
					host: t.host || "",
					port: t.port || "",
					api_key: t.api_key || ""
				});
				o[e] = (Array.isArray(r.models) ? r.models : []).map((e) => String(typeof e == "object" && e ? e.id || e.model || e.name || "" : e || "")).filter(Boolean);
			} catch (e) {
				c.value = e instanceof Error ? e.message : "Remote llama.cpp models could not be loaded.";
			} finally {
				s.value = "";
			}
		}
		return Er(() => {
			ke();
		}), (t, n) => (q(), J("section", TE, [
			Y("nav", EE, [
				Y("button", {
					type: "button",
					class: B({ active: i.value === "base" }),
					onClick: n[0] ||= (e) => i.value = "base"
				}, "Base model pool", 2),
				Y("button", {
					type: "button",
					class: B({ active: i.value === "spudex" }),
					onClick: n[1] ||= (e) => i.value = "spudex"
				}, "Spudex", 2),
				Y("button", {
					type: "button",
					class: B({ active: i.value === "beast" }),
					onClick: n[2] ||= (e) => i.value = "beast"
				}, "Beast mode", 2)
			]),
			c.value ? (q(), J("div", DE, V(c.value), 1)) : Z("", !0),
			i.value === "base" ? (q(), J(K, { key: 1 }, [
				n[99] ||= Y("article", { class: "tm-llm-route-note tm-llm-pool-note" }, [Y("i", null, "↻"), Y("div", null, [Y("strong", null, "Base routes share requests"), Y("span", null, "With multiple routes, normal Base calls rotate through the pool. Hydra roles stay assigned to a consistent pool member to keep their prompt caches warm. These routes are peers, not fallback-only backups.")])], -1),
				(q(!0), J(K, null, G(_.value, (t, r) => (q(), J("article", {
					key: r,
					class: "tm-form-card tm-llm-route-card"
				}, [
					Y("header", null, [Y("div", null, [
						Y("span", OE, "Base route " + V(r + 1), 1),
						Y("h3", null, V(r === 0 ? "Choose a Base model" : "Additional Base model"), 1),
						Y("p", null, V(_.value.length > 1 ? "This route shares normal Base requests with every other pool member." : "All normal Base requests use this route until another pool member is added."), 1)
					]), Y("div", kE, [Y("span", AE, [n[30] ||= Y("i", null, null, -1), X("Pool member " + V(r + 1), 1)]), r > 0 ? (q(), J("button", {
						key: 0,
						class: "tv-button danger",
						type: "button",
						onClick: (e) => Ce(r)
					}, "Remove", 8, jE)) : Z("", !0)])]),
					Y("section", ME, [Y("div", NE, [n[31] ||= Y("div", null, [Y("strong", null, "Provider"), Y("span", null, "Pick where this route runs. Only its matching settings appear below.")], -1), Y("b", null, V(ie(re(t)).label), 1)]), Y("div", {
						class: "tm-llm-provider-picker",
						role: "group",
						"aria-label": `Base route ${r + 1} provider`
					}, [(q(), J(K, null, G(f, (e) => Y("button", {
						key: e.value,
						type: "button",
						class: B({ active: re(t) === e.value }),
						"aria-pressed": re(t) === e.value,
						onClick: (n) => xe(t, e.value)
					}, [
						Y("i", null, V(e.mark), 1),
						Y("span", null, [Y("strong", null, V(e.label), 1), Y("small", null, V(e.short), 1)]),
						n[32] ||= Y("b", { "aria-hidden": "true" }, "✓", -1)
					], 10, FE)), 64))], 8, PE)]),
					Y("section", IE, [Y("header", null, [Y("div", null, [n[33] ||= Y("span", { class: "tv-eyebrow" }, "Route settings", -1), Y("h4", null, V(ie(re(t)).label), 1)]), Y("span", { class: B({ local: ae(re(t)) }) }, V(ae(re(t)) ? "On this Tater" : re(t) === "spud_link" ? "Paired route" : "Network connection"), 3)]), re(t) === "spud_link" ? (q(), J("div", LE, [...n[34] ||= [Y("i", null, "SP", -1), Y("div", null, [Y("strong", null, "No model address needed"), Y("span", null, "The paired Spud Hub chooses and runs the model for this request.")], -1)]])) : (q(), J("div", RE, [
						se(re(t)) ? (q(), J("label", zE, [
							n[35] ||= Y("span", { class: "tm-field-label" }, "Host or base URL", -1),
							W(Y("input", {
								"onUpdate:modelValue": (e) => t.host = e,
								type: "text",
								placeholder: "http://127.0.0.1",
								onInput: z
							}, null, 40, BE), [[$, t.host]]),
							n[36] ||= Y("small", null, "Enter the server address; add the port separately below if needed.", -1)
						])) : Z("", !0),
						se(re(t)) ? (q(), J("label", VE, [n[37] ||= Y("span", { class: "tm-field-label" }, "Port", -1), W(Y("input", {
							"onUpdate:modelValue": (e) => t.port = e,
							type: "number",
							min: "1",
							max: "65535",
							placeholder: "1234",
							onInput: z
						}, null, 40, HE), [[$, t.port]])])) : Z("", !0),
						se(re(t)) ? (q(), J("label", UE, [n[38] ||= Y("span", { class: "tm-field-label" }, "API key", -1), W(Y("input", {
							"onUpdate:modelValue": (e) => t.api_key = e,
							type: "password",
							autocomplete: "off",
							placeholder: "Optional",
							onInput: z
						}, null, 40, WE), [[$, t.api_key]])])) : Z("", !0),
						ae(re(t)) ? (q(), J("label", GE, [
							n[40] ||= Y("span", { class: "tm-field-label" }, "Downloaded model", -1),
							W(Y("select", {
								"onUpdate:modelValue": (e) => t.model = e,
								onChange: (e) => be(t)
							}, [
								n[39] ||= Y("option", { value: "" }, "Select a downloaded model", -1),
								t.model && !ve(re(t), t.model) ? (q(), J("option", {
									key: 0,
									value: t.model
								}, "Current: " + V(t.model), 9, qE)) : Z("", !0),
								(q(!0), J(K, null, G(_e(re(t)), (e) => (q(), J("option", {
									key: String(e.model),
									value: e.model
								}, V(e.model), 9, JE))), 128))
							], 40, KE), [[ds, t.model]]),
							_e(re(t)).length ? Z("", !0) : (q(), J("small", YE, "No matching model is installed yet. Download one from Hugging Face first."))
						])) : (q(), J("label", XE, [n[41] ||= Y("span", { class: "tm-field-label" }, "Model id or alias", -1), W(Y("input", {
							"onUpdate:modelValue": (e) => t.model = e,
							type: "text",
							placeholder: "Model id or server alias",
							onInput: z
						}, null, 40, ZE), [[$, t.model]])])),
						ce(re(t)) ? (q(), J("label", QE, [
							n[42] ||= Y("span", { class: "tm-field-label" }, "Prompt cache slot", -1),
							W(Y("input", {
								"onUpdate:modelValue": (e) => t.llama_cpp_slot = e,
								type: "number",
								min: "0",
								max: Math.max(0, Number(e.draft.hydra_llama_cpp_slot_count || 1) - 1),
								placeholder: "Auto",
								onInput: z
							}, null, 40, $E), [[$, t.llama_cpp_slot]]),
							n[43] ||= Y("small", null, "Blank lets Tater assign stable slots automatically. A number pins this route to that exact slot.", -1)
						])) : Z("", !0),
						oe(re(t)) ? (q(), J("div", eD, [Y("button", {
							class: "tv-button",
							type: "button",
							disabled: s.value === `base-${r}`,
							onClick: (e) => Ae(`base-${r}`, t)
						}, V(s.value === `base-${r}` ? "Looking…" : "Discover server models"), 9, tD), o[`base-${r}`]?.length ? W((q(), J("select", {
							key: 0,
							"onUpdate:modelValue": (e) => t.model = e,
							onChange: z
						}, [(q(!0), J(K, null, G(o[`base-${r}`], (e) => (q(), J("option", {
							key: e,
							value: e
						}, V(e), 9, rD))), 128))], 40, nD)), [[ds, t.model]]) : Z("", !0)])) : Z("", !0)
					]))])
				]))), 128)),
				Y("button", {
					class: "tv-button tm-add-row tm-llm-add-route",
					type: "button",
					onClick: Se
				}, "＋ Add round-robin route"),
				v.value.length ? (q(), J("article", iD, [
					Y("header", null, [Y("div", null, [
						n[44] ||= Y("span", { class: "tv-eyebrow" }, "Local runtime", -1),
						Y("h3", null, V(y.value?.label) + " settings", 1),
						n[45] ||= Y("p", null, "Only settings for a local provider used above are shown. These apply whenever Tater loads that provider.", -1)
					]), v.value.length > 1 ? (q(), J("div", aD, [(q(!0), J(K, null, G(v.value, (e) => (q(), J("button", {
						key: e.value,
						type: "button",
						class: B({ active: a.value === e.value }),
						onClick: (t) => a.value = e.value
					}, [Y("i", null, V(e.mark), 1), X(V(e.label), 1)], 10, oD))), 128))])) : Z("", !0)]),
					Y("section", { class: B(["tm-llm-context-card", M.value]) }, [Y("header", null, [Y("div", null, [
						n[46] ||= Y("span", { class: "tv-eyebrow" }, "Context estimator", -1),
						n[47] ||= Y("h4", null, "How much working memory does this model need?", -1),
						Y("p", null, V(x.value ? `Estimate for ${x.value}` : "Choose a model above to include its detected context limit."), 1)
					]), Y("button", {
						class: "tv-button",
						type: "button",
						disabled: u.value,
						onClick: ke
					}, V(u.value ? "Refreshing…" : "Refresh estimate"), 9, sD)]), Y("div", cD, [Y("div", lD, [
						Y("div", null, [n[48] ||= Y("strong", null, "Context length", -1), Y("b", null, V(De(w.value)) + " tokens", 1)]),
						Y("input", {
							type: "range",
							min: b.value.min,
							max: C.value,
							step: "256",
							value: w.value,
							onInput: Te
						}, null, 40, uD),
						Y("label", null, [n[49] ||= Y("span", null, "Exact token limit", -1), Y("input", {
							type: "number",
							min: b.value.min,
							max: C.value,
							step: "256",
							value: w.value,
							onInput: Te
						}, null, 40, dD)]),
						Y("small", null, V(S.value?.max_context_tokens ? `Model maximum ${De(C.value)} from ${S.value.context_source === "gguf" ? "GGUF metadata" : "model config"}.` : `Model maximum is unknown; Tater uses a safe ${De(C.value)} slider cap.`), 1)
					]), Y("div", fD, [
						Y("div", null, [n[50] ||= Y("span", null, [Y("i"), X("Context fit")], -1), Y("strong", null, V(N.value), 1)]),
						Y("div", pD, [Y("span", { style: R({ width: `${P.value}%` }) }, null, 4), Y("i", { style: R({ left: `${F.value}%` }) }, null, 4)]),
						Y("h4", null, V(D.value ? `Recommended: ${De(D.value)} tokens` : "Building a recommendation"), 1),
						Y("p", null, V(Oe()), 1),
						Y("div", mD, [
							Y("span", null, [n[51] ||= X("Prompt ", -1), Y("b", null, V(De(O.value)), 1)]),
							Y("span", null, [n[52] ||= X("Reply ", -1), Y("b", null, V(De(k.value)), 1)]),
							Y("span", null, [n[53] ||= X("Minimum ", -1), Y("b", null, V(De(E.value)), 1)]),
							Y("span", null, [n[54] ||= X("Reserve ", -1), Y("b", null, V(De(A.value)), 1)])
						])
					])])], 2),
					a.value === "hf_transformers" ? (q(), J(K, { key: 0 }, [Y("section", hD, [n[67] ||= Y("header", null, [Y("div", null, [Y("h4", null, "Execution"), Y("p", null, "Choose how Transformers places and computes the model.")]), Y("span", { class: "tm-llm-section-chip" }, "Transformers")], -1), Y("div", gD, [
						Y("label", _D, [
							n[56] ||= Y("span", { class: "tm-field-label" }, "Device", -1),
							W(Y("select", {
								"onUpdate:modelValue": n[3] ||= (t) => e.draft.hydra_hf_transformers_device = t,
								onChange: z
							}, [...n[55] ||= [
								Y("option", { value: "auto" }, "Auto", -1),
								Y("option", { value: "cuda" }, "CUDA", -1),
								Y("option", { value: "mps" }, "Apple MPS", -1),
								Y("option", { value: "cpu" }, "CPU", -1)
							]], 544), [[ds, e.draft.hydra_hf_transformers_device]]),
							n[57] ||= Y("small", null, "Auto tries CUDA, Apple MPS, then CPU.", -1)
						]),
						Y("label", vD, [
							n[59] ||= Y("span", { class: "tm-field-label" }, "Precision", -1),
							W(Y("select", {
								"onUpdate:modelValue": n[4] ||= (t) => e.draft.hydra_hf_transformers_dtype = t,
								onChange: z
							}, [...n[58] ||= [
								Y("option", { value: "auto" }, "Auto", -1),
								Y("option", { value: "float16" }, "Float16", -1),
								Y("option", { value: "bfloat16" }, "BFloat16", -1),
								Y("option", { value: "float32" }, "Float32", -1)
							]], 544), [[ds, e.draft.hydra_hf_transformers_dtype]]),
							n[60] ||= Y("small", null, "Auto uses the model's recommended data type.", -1)
						]),
						Y("label", yD, [
							n[62] ||= Y("span", { class: "tm-field-label" }, "Device map", -1),
							W(Y("select", {
								"onUpdate:modelValue": n[5] ||= (t) => e.draft.hydra_hf_transformers_device_map = t,
								onChange: z
							}, [...n[61] ||= [
								Y("option", { value: "default" }, "Default", -1),
								Y("option", { value: "auto" }, "Auto", -1),
								Y("option", { value: "balanced" }, "Balanced", -1),
								Y("option", { value: "disabled" }, "Disabled", -1)
							]], 544), [[ds, e.draft.hydra_hf_transformers_device_map]]),
							n[63] ||= Y("small", null, "Controls how layers are distributed across devices.", -1)
						]),
						Y("label", bD, [
							n[65] ||= Y("span", { class: "tm-field-label" }, "Attention", -1),
							W(Y("select", {
								"onUpdate:modelValue": n[6] ||= (t) => e.draft.hydra_hf_transformers_attn_implementation = t,
								onChange: z
							}, [...n[64] ||= [
								Y("option", { value: "auto" }, "Auto", -1),
								Y("option", { value: "sdpa" }, "SDPA", -1),
								Y("option", { value: "flash_attention_2" }, "Flash Attention 2", -1),
								Y("option", { value: "eager" }, "Eager", -1)
							]], 544), [[ds, e.draft.hydra_hf_transformers_attn_implementation]]),
							n[66] ||= Y("small", null, "Use Auto unless a model requires a specific implementation.", -1)
						])
					])]), Y("label", xD, [
						W(Y("input", {
							"onUpdate:modelValue": n[7] ||= (t) => e.draft.hydra_hf_transformers_trust_remote_code = t,
							type: "checkbox",
							onChange: z
						}, null, 544), [[cs, e.draft.hydra_hf_transformers_trust_remote_code]]),
						n[68] ||= Y("span", null, [Y("strong", null, "Trust remote model code"), Y("small", null, "Allow custom Python code from the selected repository. Enable only for models you trust.")], -1),
						Y("b", null, V(e.draft.hydra_hf_transformers_trust_remote_code ? "On" : "Off"), 1)
					])], 64)) : a.value === "llama_cpp" ? (q(), J(K, { key: 1 }, [Y("section", SD, [
						n[77] ||= Y("header", null, [Y("div", null, [Y("h4", null, "Performance"), Y("p", null, "Start with the defaults, then tune only when you need more throughput or concurrent slots.")]), Y("span", { class: "tm-llm-section-chip" }, "GGUF")], -1),
						Y("div", CD, [
							Y("label", wD, [
								n[69] ||= Y("span", { class: "tm-field-label" }, "Concurrent slots", -1),
								W(Y("input", {
									"onUpdate:modelValue": n[8] ||= (t) => e.draft.hydra_llama_cpp_slot_count = t,
									type: "number",
									min: "1",
									max: "32",
									onInput: z
								}, null, 544), [[$, e.draft.hydra_llama_cpp_slot_count]]),
								n[70] ||= Y("small", null, "How many llama.cpp requests can run at once.", -1)
							]),
							Y("label", TD, [
								n[71] ||= Y("span", { class: "tm-field-label" }, "Evaluation batch", -1),
								W(Y("input", {
									"onUpdate:modelValue": n[9] ||= (t) => e.draft.hydra_llama_cpp_n_batch = t,
									type: "number",
									min: "32",
									max: "8192",
									step: "32",
									onInput: z
								}, null, 544), [[$, e.draft.hydra_llama_cpp_n_batch]]),
								n[72] ||= Y("small", null, "Higher can speed prompt processing when memory allows.", -1)
							]),
							Y("label", ED, [
								n[73] ||= Y("span", { class: "tm-field-label" }, "Micro-batch", -1),
								W(Y("input", {
									"onUpdate:modelValue": n[10] ||= (t) => e.draft.hydra_llama_cpp_n_ubatch = t,
									type: "number",
									min: "0",
									max: "8192",
									step: "32",
									placeholder: "Auto",
									onInput: z
								}, null, 544), [[$, e.draft.hydra_llama_cpp_n_ubatch]]),
								n[74] ||= Y("small", null, "0 lets llama.cpp choose, usually matching the evaluation batch.", -1)
							])
						]),
						Y("div", DD, [Y("label", OD, [
							W(Y("input", {
								"onUpdate:modelValue": n[11] ||= (t) => e.draft.hydra_llama_cpp_flash_attn = t,
								type: "checkbox",
								onChange: z
							}, null, 544), [[cs, e.draft.hydra_llama_cpp_flash_attn]]),
							n[75] ||= Y("span", null, [Y("strong", null, "Flash attention"), Y("small", null, "Faster attention when supported by the model and backend.")], -1),
							Y("b", null, V(e.draft.hydra_llama_cpp_flash_attn ? "On" : "Off"), 1)
						]), Y("label", kD, [
							W(Y("input", {
								"onUpdate:modelValue": n[12] ||= (t) => e.draft.hydra_llama_cpp_offload_kqv = t,
								type: "checkbox",
								onChange: z
							}, null, 544), [[cs, e.draft.hydra_llama_cpp_offload_kqv]]),
							n[76] ||= Y("span", null, [Y("strong", null, "GPU KV offload"), Y("small", null, "Keep attention and KV-cache work on the GPU when supported.")], -1),
							Y("b", null, V(e.draft.hydra_llama_cpp_offload_kqv ? "On" : "Off"), 1)
						])])
					]), Y("section", { class: B(["tm-llm-speculative", { enabled: e.draft.hydra_llama_cpp_mtp_enabled }]) }, [Y("header", null, [n[79] ||= Y("div", null, [
						Y("span", { class: "tv-eyebrow" }, "Speed boost"),
						Y("h4", null, "Speculative decoding"),
						Y("p", null, "A fast draft predicts tokens and the main model verifies them. It can improve generation speed with a compatible model pair.")
					], -1), Y("label", AD, [
						W(Y("input", {
							"onUpdate:modelValue": n[13] ||= (t) => e.draft.hydra_llama_cpp_mtp_enabled = t,
							type: "checkbox",
							onChange: z
						}, null, 544), [[cs, e.draft.hydra_llama_cpp_mtp_enabled]]),
						n[78] ||= Y("span", null, [Y("i")], -1),
						Y("b", null, V(e.draft.hydra_llama_cpp_mtp_enabled ? "Enabled" : "Disabled"), 1)
					])]), e.draft.hydra_llama_cpp_mtp_enabled ? (q(), J("div", jD, [
						Y("label", MD, [
							n[81] ||= Y("span", { class: "tm-field-label" }, "Draft method", -1),
							W(Y("select", {
								"onUpdate:modelValue": n[14] ||= (t) => e.draft.hydra_llama_cpp_speculative_method = t,
								onChange: Ee
							}, [...n[80] ||= [
								Y("option", { value: "draft-mtp" }, "Multi-Token Prediction (MTP)", -1),
								Y("option", { value: "draft-dflash" }, "DFlash", -1),
								Y("option", { value: "draft-dspark" }, "DSpark", -1)
							]], 544), [[ds, e.draft.hydra_llama_cpp_speculative_method]]),
							Y("small", null, V(I.value.help), 1)
						]),
						Y("label", ND, [
							n[82] ||= Y("span", { class: "tm-field-label" }, "Draft model (GGUF)", -1),
							W(Y("select", {
								"onUpdate:modelValue": n[15] ||= (t) => e.draft.hydra_llama_cpp_mtp_draft_model = t,
								onChange: z
							}, [Y("option", PD, V(I.value.requiresDraft ? "Choose a compatible draft model" : "Embedded heads / no sidecar"), 1), (q(!0), J(K, null, G(te.value, (e) => (q(), J("option", {
								key: String(e.model),
								value: e.model
							}, V(e.filename || e.model), 9, FD))), 128))], 544), [[ds, e.draft.hydra_llama_cpp_mtp_draft_model]]),
							ee.value ? te.value.length ? (q(), J("small", LD, V(te.value.length) + " compatible " + V(I.value.label) + " " + V(te.value.length === 1 ? "model" : "models") + " for " + V(L.value) + ".", 1)) : (q(), J("small", RD, "No compatible " + V(I.value.label) + " sidecar is installed for " + V(L.value) + ".", 1)) : (q(), J("small", ID, "Choose a llama.cpp base model above first.")),
							ne.value ? Z("", !0) : (q(), J("small", zD, "The previous draft did not match this method or base model and will not be used."))
						]),
						Y("label", BD, [
							n[83] ||= Y("span", { class: "tm-field-label" }, "Maximum draft tokens", -1),
							Y("div", null, [W(Y("input", {
								"onUpdate:modelValue": n[16] ||= (t) => e.draft.hydra_llama_cpp_mtp_draft_tokens = t,
								type: "range",
								min: "1",
								max: "16",
								step: "1",
								onInput: z
							}, null, 544), [[$, e.draft.hydra_llama_cpp_mtp_draft_tokens]]), W(Y("input", {
								"onUpdate:modelValue": n[17] ||= (t) => e.draft.hydra_llama_cpp_mtp_draft_tokens = t,
								type: "number",
								min: "1",
								max: "16",
								step: "1",
								onInput: z
							}, null, 544), [[$, e.draft.hydra_llama_cpp_mtp_draft_tokens]])]),
							Y("small", null, V(I.value.label) + " recommends " + V(I.value.tokens) + ".", 1)
						])
					])) : (q(), J("div", VD, [...n[84] ||= [Y("i", null, "↗", -1), Y("span", null, [Y("strong", null, "Optional advanced feature"), Y("small", null, "Leave this off for the simplest, most compatible llama.cpp setup.")], -1)]]))], 2)], 64)) : a.value === "mlx_lm" ? (q(), J("section", HD, [
						n[97] ||= Y("header", null, [Y("div", null, [Y("h4", null, "Memory and loading"), Y("p", null, "Tune MLX prefill and KV cache use for Apple Silicon.")]), Y("span", { class: "tm-llm-section-chip" }, "Apple Silicon")], -1),
						Y("div", UD, [
							Y("label", WD, [
								n[85] ||= Y("span", { class: "tm-field-label" }, "Prefill step size", -1),
								W(Y("input", {
									"onUpdate:modelValue": n[18] ||= (t) => e.draft.hydra_mlx_engine_prefill_step_size = t,
									type: "number",
									min: "1",
									max: "32768",
									placeholder: "Auto",
									onInput: z
								}, null, 544), [[$, e.draft.hydra_mlx_engine_prefill_step_size]]),
								n[86] ||= Y("small", null, "Blank lets Tater choose from available Mac memory.", -1)
							]),
							Y("label", GD, [
								n[88] ||= Y("span", { class: "tm-field-label" }, "Quantized KV bits", -1),
								W(Y("select", {
									"onUpdate:modelValue": n[19] ||= (t) => e.draft.hydra_mlx_engine_kv_bits = t,
									onChange: z
								}, [n[87] ||= Y("option", { value: "" }, "Auto", -1), (q(), J(K, null, G([
									"2",
									"3",
									"4",
									"6",
									"8"
								], (e) => Y("option", {
									key: e,
									value: e
								}, V(e) + "-bit", 9, KD)), 64))], 544), [[ds, e.draft.hydra_mlx_engine_kv_bits]]),
								n[89] ||= Y("small", null, "Lower values reduce memory use at a possible quality cost.", -1)
							]),
							Y("label", qD, [
								n[91] ||= Y("span", { class: "tm-field-label" }, "KV group size", -1),
								W(Y("select", {
									"onUpdate:modelValue": n[20] ||= (t) => e.draft.hydra_mlx_engine_kv_group_size = t,
									onChange: z
								}, [...n[90] ||= [
									Y("option", { value: "" }, "Auto", -1),
									Y("option", { value: "32" }, "32", -1),
									Y("option", { value: "64" }, "64", -1),
									Y("option", { value: "128" }, "128", -1)
								]], 544), [[ds, e.draft.hydra_mlx_engine_kv_group_size]]),
								n[92] ||= Y("small", null, "Auto uses the MLX runtime default.", -1)
							]),
							Y("label", JD, [
								n[93] ||= Y("span", { class: "tm-field-label" }, "Quantized KV start", -1),
								W(Y("input", {
									"onUpdate:modelValue": n[21] ||= (t) => e.draft.hydra_mlx_engine_quantized_kv_start = t,
									type: "number",
									min: "0",
									placeholder: "Auto",
									onInput: z
								}, null, 544), [[$, e.draft.hydra_mlx_engine_quantized_kv_start]]),
								n[94] ||= Y("small", null, "The token index where quantized KV begins.", -1)
							])
						]),
						Y("div", YD, [Y("label", XD, [
							W(Y("input", {
								"onUpdate:modelValue": n[22] ||= (t) => e.draft.hydra_mlx_lm_lazy_load = t,
								type: "checkbox",
								onChange: z
							}, null, 544), [[cs, e.draft.hydra_mlx_lm_lazy_load]]),
							n[95] ||= Y("span", null, [Y("strong", null, "Lazy loading"), Y("small", null, "Defer some weight materialization while loading the model.")], -1),
							Y("b", null, V(e.draft.hydra_mlx_lm_lazy_load ? "On" : "Off"), 1)
						]), Y("label", ZD, [
							W(Y("input", {
								"onUpdate:modelValue": n[23] ||= (t) => e.draft.hydra_mlx_lm_trust_remote_code = t,
								type: "checkbox",
								onChange: z
							}, null, 544), [[cs, e.draft.hydra_mlx_lm_trust_remote_code]]),
							n[96] ||= Y("span", null, [Y("strong", null, "Trust remote model code"), Y("small", null, "Allow custom tokenizer or config code from trusted repositories.")], -1),
							Y("b", null, V(e.draft.hydra_mlx_lm_trust_remote_code ? "On" : "Off"), 1)
						])])
					])) : Z("", !0)
				])) : (q(), J("article", QD, [...n[98] ||= [Y("i", null, "✓", -1), Y("div", null, [Y("h3", null, "No local runtime to tune"), Y("p", null, "The selected routes run on an API, remote server, or paired Spud Hub. Their connection settings are already shown above.")], -1)]]))
			], 64)) : i.value === "spudex" ? (q(), J(K, { key: 2 }, [Y("article", $D, [n[102] ||= Y("header", null, [Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Spudex model"),
				Y("h3", null, "Choose how Spudex thinks"),
				Y("p", null, "Share Tater's Base route for a simple setup, or give coding work a dedicated model.")
			])], -1), Y("div", eO, [Y("button", {
				type: "button",
				class: B({ active: !e.draft.spudex_llm_provider }),
				onClick: n[24] ||= (e) => we("base")
			}, [...n[100] ||= [Y("i", null, "1", -1), Y("span", null, [Y("strong", null, "Use Base model"), Y("small", null, "Recommended for most setups")], -1)]], 2), Y("button", {
				type: "button",
				class: B({ active: !!e.draft.spudex_llm_provider }),
				onClick: n[25] ||= (e) => we("dedicated")
			}, [...n[101] ||= [Y("i", null, "2", -1), Y("span", null, [Y("strong", null, "Dedicated model"), Y("small", null, "Separate model just for Spudex")], -1)]], 2)])]), e.draft.spudex_llm_provider ? (q(), J("article", tO, [
				n[108] ||= Y("header", null, [Y("div", null, [Y("h3", null, "Dedicated Spudex route"), Y("p", null, "Only settings for the selected provider are shown.")])], -1),
				Y("div", nO, [(q(), J(K, null, G(f, (t) => Y("button", {
					key: t.value,
					type: "button",
					class: B({ active: e.draft.spudex_llm_provider === t.value }),
					onClick: (e) => H("spudex_llm_provider", t.value)
				}, [
					Y("i", null, V(t.mark), 1),
					Y("span", null, [Y("strong", null, V(t.label), 1), Y("small", null, V(t.short), 1)]),
					n[103] ||= Y("b", { "aria-hidden": "true" }, "✓", -1)
				], 10, rO)), 64))]),
				Y("div", iO, [se(e.draft.spudex_llm_provider) ? (q(), J("label", aO, [n[104] ||= Y("span", { class: "tm-field-label" }, "Host or base URL", -1), W(Y("input", {
					"onUpdate:modelValue": n[26] ||= (t) => e.draft.spudex_llm_host = t,
					type: "text",
					placeholder: "http://127.0.0.1:1234",
					onInput: z
				}, null, 544), [[$, e.draft.spudex_llm_host]])])) : Z("", !0), ae(e.draft.spudex_llm_provider) ? (q(), J("label", oO, [n[106] ||= Y("span", { class: "tm-field-label" }, "Downloaded model", -1), W(Y("select", {
					"onUpdate:modelValue": n[27] ||= (t) => e.draft.spudex_llm_model = t,
					onChange: z
				}, [
					n[105] ||= Y("option", { value: "" }, "Select a downloaded model", -1),
					e.draft.spudex_llm_model && !ve(e.draft.spudex_llm_provider, e.draft.spudex_llm_model) ? (q(), J("option", {
						key: 0,
						value: e.draft.spudex_llm_model
					}, "Current: " + V(e.draft.spudex_llm_model), 9, sO)) : Z("", !0),
					(q(!0), J(K, null, G(_e(e.draft.spudex_llm_provider), (e) => (q(), J("option", {
						key: String(e.model),
						value: e.model
					}, V(e.model), 9, cO))), 128))
				], 544), [[ds, e.draft.spudex_llm_model]])])) : e.draft.spudex_llm_provider === "spud_link" ? Z("", !0) : (q(), J("label", lO, [n[107] ||= Y("span", { class: "tm-field-label" }, "Model id or alias", -1), W(Y("input", {
					"onUpdate:modelValue": n[28] ||= (t) => e.draft.spudex_llm_model = t,
					type: "text",
					onInput: z
				}, null, 544), [[$, e.draft.spudex_llm_model]])]))])
			])) : Z("", !0)], 64)) : (q(), J(K, { key: 3 }, [Y("article", uO, [Y("header", null, [n[110] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Advanced routing"),
				Y("h3", null, "Beast Mode"),
				Y("p", null, "Give planning, critique, and final responses their own models. Keep this disabled unless you intentionally run a multi-model setup.")
			], -1), Y("label", dO, [
				W(Y("input", {
					"onUpdate:modelValue": n[29] ||= (t) => e.draft.hydra_beast_mode_enabled = t,
					type: "checkbox",
					onChange: z
				}, null, 544), [[cs, e.draft.hydra_beast_mode_enabled]]),
				n[109] ||= Y("span", null, [Y("i")], -1),
				Y("b", null, V(e.draft.hydra_beast_mode_enabled ? "Enabled" : "Disabled"), 1)
			])])]), (q(), J(K, null, G(p, (t) => Y("article", {
				key: t.id,
				class: B(["tm-form-card tm-llm-role-card", { muted: !e.draft.hydra_beast_mode_enabled }])
			}, [
				Y("header", null, [Y("div", null, [
					n[111] ||= Y("span", { class: "tv-eyebrow" }, "Beast role", -1),
					Y("h3", null, V(t.label), 1),
					Y("p", null, V(t.description), 1)
				]), Y("span", fO, V(ie(e.draft[le(t.id, "provider")]).label), 1)]),
				Y("div", {
					class: "tm-llm-provider-picker compact",
					role: "group",
					"aria-label": `${t.label} provider`
				}, [(q(), J(K, null, G(f, (r) => Y("button", {
					key: r.value,
					type: "button",
					disabled: !e.draft.hydra_beast_mode_enabled,
					class: B({ active: e.draft[le(t.id, "provider")] === r.value }),
					onClick: (e) => H(le(t.id, "provider"), r.value)
				}, [
					Y("i", null, V(r.mark), 1),
					Y("span", null, [Y("strong", null, V(r.label), 1), Y("small", null, V(r.short), 1)]),
					n[112] ||= Y("b", { "aria-hidden": "true" }, "✓", -1)
				], 10, mO)), 64))], 8, pO),
				Y("div", hO, [
					se(e.draft[le(t.id, "provider")]) ? (q(), J("label", gO, [n[113] ||= Y("span", { class: "tm-field-label" }, "Host or base URL", -1), W(Y("input", {
						"onUpdate:modelValue": (n) => e.draft[le(t.id, "host")] = n,
						type: "text",
						onInput: z
					}, null, 40, _O), [[$, e.draft[le(t.id, "host")]]])])) : Z("", !0),
					se(e.draft[le(t.id, "provider")]) ? (q(), J("label", vO, [n[114] ||= Y("span", { class: "tm-field-label" }, "Port", -1), W(Y("input", {
						"onUpdate:modelValue": (n) => e.draft[le(t.id, "port")] = n,
						type: "number",
						min: "1",
						max: "65535",
						onInput: z
					}, null, 40, yO), [[$, e.draft[le(t.id, "port")]]])])) : Z("", !0),
					se(e.draft[le(t.id, "provider")]) ? (q(), J("label", bO, [n[115] ||= Y("span", { class: "tm-field-label" }, "API key", -1), W(Y("input", {
						"onUpdate:modelValue": (n) => e.draft[le(t.id, "api_key")] = n,
						type: "password",
						onInput: z
					}, null, 40, xO), [[$, e.draft[le(t.id, "api_key")]]])])) : Z("", !0),
					ae(e.draft[le(t.id, "provider")]) ? (q(), J("label", SO, [n[117] ||= Y("span", { class: "tm-field-label" }, "Downloaded model", -1), W(Y("select", {
						"onUpdate:modelValue": (n) => e.draft[le(t.id, "model")] = n,
						onChange: z
					}, [
						n[116] ||= Y("option", { value: "" }, "Select a downloaded model", -1),
						e.draft[le(t.id, "model")] && !ve(e.draft[le(t.id, "provider")], e.draft[le(t.id, "model")]) ? (q(), J("option", {
							key: 0,
							value: e.draft[le(t.id, "model")]
						}, "Current: " + V(e.draft[le(t.id, "model")]), 9, wO)) : Z("", !0),
						(q(!0), J(K, null, G(_e(e.draft[le(t.id, "provider")]), (e) => (q(), J("option", {
							key: String(e.model),
							value: e.model
						}, V(e.model), 9, TO))), 128))
					], 40, CO), [[ds, e.draft[le(t.id, "model")]]])])) : e.draft[le(t.id, "provider")] === "spud_link" ? Z("", !0) : (q(), J("label", EO, [n[118] ||= Y("span", { class: "tm-field-label" }, "Model id or alias", -1), W(Y("input", {
						"onUpdate:modelValue": (n) => e.draft[le(t.id, "model")] = n,
						type: "text",
						onInput: z
					}, null, 40, DO), [[$, e.draft[le(t.id, "model")]]])])),
					ce(e.draft[le(t.id, "provider")]) ? (q(), J("label", OO, [
						n[119] ||= Y("span", { class: "tm-field-label" }, "Prompt cache slot", -1),
						W(Y("input", {
							"onUpdate:modelValue": (n) => e.draft[le(t.id, "llama_cpp_slot")] = n,
							type: "number",
							min: "0",
							max: Math.max(0, Number(e.draft.hydra_llama_cpp_slot_count || 1) - 1),
							placeholder: "Auto",
							onInput: z
						}, null, 40, kO), [[$, e.draft[le(t.id, "llama_cpp_slot")]]]),
						n[120] ||= Y("small", null, "Blank gives this role a stable automatic slot. A number pins it to that exact slot.", -1)
					])) : Z("", !0)
				])
			], 2)), 64))], 64))
		]));
	}
}), jO = { class: "tm-model-apply-hero" }, MO = { class: "tm-model-apply-overall" }, NO = ["aria-valuenow"], PO = {
	class: "tm-model-apply-stages",
	"aria-label": "Model apply stages"
}, FO = { "aria-hidden": "true" }, IO = {
	key: 1,
	class: "tm-model-apply-items"
}, LO = { class: "tm-model-item-track" }, RO = {
	key: 2,
	class: "tm-model-apply-errors",
	role: "alert"
}, zO = /* @__PURE__ */ sr({
	__name: "LocalModelApplyProgress",
	props: {
		open: { type: Boolean },
		snapshot: {},
		pollError: {}
	},
	emits: ["close"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = Q(() => Array.isArray(n.snapshot.items) ? n.snapshot.items : []), a = Q(() => [...Array.isArray(n.snapshot.errors) ? n.snapshot.errors.map(String) : [], ...n.pollError ? [n.pollError] : []].filter(Boolean)), o = Q(() => !!n.snapshot.running), s = Q(() => String(n.snapshot.ui_phase || "") === "saving"), c = Q(() => String(n.snapshot.active_key || "")), l = Q(() => i.value.find((e) => !!e.active) || i.value.find((e) => [
			"preparing",
			"downloading",
			"loading",
			"working",
			"cancelling"
		].includes(w(e))) || null), u = Q(() => i.value.length > 0 && i.value.every((e) => [
			"loaded",
			"downloaded",
			"error",
			"cancelled",
			"canceled"
		].includes(w(e)))), d = Q(() => n.snapshot.runtime_restart && typeof n.snapshot.runtime_restart == "object" ? n.snapshot.runtime_restart : {}), f = Q(() => !!d.value.running || c.value === "__runtime_restart__"), p = Q(() => Number(d.value.finished_ts || 0) > 0), m = Q(() => !s.value && !o.value), h = Q(() => m.value && !a.value.length), g = Q(() => s.value ? 6 : c.value === "__unload_previous__" ? 14 : f.value ? 94 : m.value ? a.value.length ? Math.max(12, _.value) : 100 : Math.max(18, Math.min(91, 18 + _.value * .72))), _ = Q(() => i.value.length ? i.value.reduce((e, t) => e + C(t.progress), 0) / i.value.length : 0), v = Q(() => a.value.length && m.value ? "Model change needs attention" : s.value ? "Saving your model setup" : c.value === "__unload_previous__" ? "Unloading the previous model" : f.value ? "Refreshing Cores and Portals" : l.value ? `Loading ${String(l.value.provider_label || E(l.value.provider))}` : h.value ? "Tater is ready" : "Preparing local models"), y = Q(() => a.value.length && m.value ? a.value[0] : s.value ? "Writing the provider, routing, and runtime settings you selected." : c.value === "__unload_previous__" ? "Tater is releasing the old model cleanly before loading the new one." : f.value ? "The selected model is ready. Running surfaces are reconnecting to it now." : l.value ? D(l.value) : h.value ? "Models, Cores, and Portals are synchronized with your saved settings." : "Tater is preparing the selected local runtime."), b = Q(() => s.value ? "Saving settings…" : c.value === "__unload_previous__" ? "Unloading previous local models…" : f.value ? "Restarting running Cores and Portals…" : l.value ? String(l.value.message || T(l.value.status)) : a.value.length ? a.value[0] : h.value ? "Model change complete" : "Waiting for the model loader…"), x = Q(() => {
			let e = Array.isArray(n.snapshot.unload_before) ? n.snapshot.unload_before : [], t = n.snapshot.unload_result && typeof n.snapshot.unload_result == "object" ? n.snapshot.unload_result : {}, r = Array.isArray(t.errors) ? t.errors : [], o = i.value.filter((e) => w(e) === "error"), g = i.value.filter((e) => ["loaded", "downloaded"].includes(w(e))).length, _ = k(d.value), v = f.value || p.value || Number(d.value.started_ts || 0) > 0;
			return [
				{
					id: "save",
					label: "Save settings",
					detail: s.value ? "Writing model and runtime choices." : "Settings accepted.",
					state: s.value ? "active" : "done"
				},
				{
					id: "unload",
					label: "Unload previous",
					detail: e.length ? c.value === "__unload_previous__" ? `${e.length} previous ${e.length === 1 ? "model" : "models"} releasing.` : `${Number(t.unloaded_count || e.length)} previous ${e.length === 1 ? "model" : "models"} handled.` : "No previous local model to release.",
					state: c.value === "__unload_previous__" ? "active" : r.length ? "error" : s.value ? "pending" : e.length ? "done" : "skipped"
				},
				{
					id: "load",
					label: "Load selected",
					detail: i.value.length ? l.value ? D(l.value) : `${g}/${i.value.length} ${i.value.length === 1 ? "model" : "models"} ready.` : s.value ? "Waiting for saved model choices." : "No local model load was needed.",
					state: o.length ? "error" : l.value ? "active" : u.value ? "done" : i.value.length || s.value ? "pending" : "skipped"
				},
				{
					id: "restart",
					label: "Refresh platforms",
					detail: v ? A(d.value) : i.value.length ? "Runs after the selected model is ready." : "No running platform refresh was needed.",
					state: _ || d.value.error ? "error" : f.value ? "active" : p.value ? "done" : m.value ? "skipped" : "pending"
				},
				{
					id: "ready",
					label: a.value.length ? "Needs attention" : "Ready",
					detail: a.value.length ? "Review the message below before trying again." : "The saved model setup is ready to use.",
					state: a.value.length && m.value ? "error" : h.value ? "done" : "pending"
				}
			];
		}), S = Q(() => f.value || p.value || !!d.value.error);
		function C(e) {
			return Math.max(0, Math.min(100, Number(e) || 0));
		}
		function w(e) {
			return String(e.status || "pending").trim().toLowerCase();
		}
		function T(e) {
			let t = String(e || "pending").replaceAll("_", " ");
			return t.charAt(0).toUpperCase() + t.slice(1);
		}
		function E(e) {
			return {
				llama_cpp: "llama.cpp",
				hf_transformers: "Transformers",
				mlx_lm: "MLX LM"
			}[String(e || "")] || "local model";
		}
		function D(e) {
			let t = String(e.filename || e.model || "Selected local model");
			return t.split("::").pop() || t;
		}
		function O(e) {
			return Array.isArray(e) ? e.length : e && typeof e == "object" ? Object.keys(e).length : 0;
		}
		function k(e) {
			let t = e.stopped && typeof e.stopped == "object" ? e.stopped : {}, n = e.resumed && typeof e.resumed == "object" ? e.resumed : {}, r = (e, t) => e[t] && typeof e[t] == "object" ? e[t] : {};
			return O(r(t, "cores").failed) + O(r(t, "portals").failed) + O(r(n, "cores").failed) + O(r(n, "portals").failed);
		}
		function A(e) {
			if (e.error) return String(e.error);
			let t = e.active_before && typeof e.active_before == "object" ? e.active_before : {}, n = O(t.cores), r = O(t.portals), i = k(e);
			return i ? `${i} platform refresh ${i === 1 ? "step needs" : "steps need"} attention.` : f.value ? `${n} ${n === 1 ? "Core" : "Cores"} and ${r} ${r === 1 ? "Portal" : "Portals"} reconnecting.` : `${n} ${n === 1 ? "Core" : "Cores"} and ${r} ${r === 1 ? "Portal" : "Portals"} refreshed.`;
		}
		return (t, n) => (q(), da(kl, {
			open: e.open,
			"backdrop-class": "tv-modal-backdrop tset-modal tm-model-apply-backdrop",
			onClose: n[2] ||= (e) => r("close")
		}, {
			default: Sn(() => [Y("section", {
				class: B(["tv-modal tm-model-apply", {
					running: o.value,
					complete: m.value,
					error: a.value.length
				}]),
				role: "dialog",
				"aria-modal": "true",
				"aria-labelledby": "tm-model-apply-title"
			}, [
				Y("header", null, [n[3] ||= Y("div", null, [
					Y("span", { class: "tv-eyebrow" }, "Tater model loader"),
					Y("h2", { id: "tm-model-apply-title" }, "Apply local model changes"),
					Y("p", null, "Live progress from the model runtime and every surface connected to it.")
				], -1), Y("button", {
					class: "tv-button",
					type: "button",
					onClick: n[0] ||= (e) => r("close")
				}, V(o.value || s.value ? "Hide" : "Close"), 1)]),
				Y("section", jO, [n[4] ||= Y("div", {
					class: "tm-model-spud",
					"aria-hidden": "true"
				}, [
					Y("span", { class: "body" }),
					Y("span", { class: "visor" }),
					Y("span", { class: "glow" }),
					Y("i", { class: "spark one" }),
					Y("i", { class: "spark two" }),
					Y("i", { class: "spark three" })
				], -1), Y("div", null, [
					Y("span", null, V(m.value ? a.value.length ? "Stopped" : "Complete" : "Working"), 1),
					Y("strong", null, V(v.value), 1),
					Y("p", null, V(y.value), 1)
				])]),
				Y("div", MO, [
					Y("div", null, [Y("strong", null, V(b.value), 1), Y("b", null, V(Math.round(g.value)) + "%", 1)]),
					Y("div", {
						class: "tm-model-apply-track",
						role: "progressbar",
						"aria-valuenow": Math.round(g.value),
						"aria-valuemin": "0",
						"aria-valuemax": "100"
					}, [Y("span", { style: R({ width: `${g.value}%` }) }, null, 4)], 8, NO),
					Y("small", null, V(o.value || s.value ? "This continues safely if you hide the window or change tabs." : a.value.length ? "The saved settings remain available; fix the reported issue and apply again." : "All model work for this save has finished."), 1)
				]),
				Y("div", PO, [(q(!0), J(K, null, G(x.value, (e) => (q(), J("article", {
					key: e.id,
					class: B(e.state)
				}, [Y("i", FO, V(e.state === "done" ? "✓" : e.state === "error" ? "!" : e.state === "skipped" ? "–" : ""), 1), Y("div", null, [Y("strong", null, V(e.label), 1), Y("span", null, V(e.detail), 1)])], 2))), 128))]),
				S.value ? (q(), J("section", {
					key: 0,
					class: B(["tm-model-restart-summary", {
						active: f.value,
						error: d.value.error || k(d.value)
					}])
				}, [n[5] ||= Y("i", { "aria-hidden": "true" }, "↻", -1), Y("div", null, [Y("strong", null, V(f.value ? "Cores and Portals are restarting" : d.value.error || k(d.value) ? "Platform refresh needs attention" : "Cores and Portals refreshed"), 1), Y("span", null, V(A(d.value)), 1)])], 2)) : Z("", !0),
				i.value.length ? (q(), J("div", IO, [(q(!0), J(K, null, G(i.value, (e) => (q(), J("article", {
					key: String(e.key || e.model),
					class: B(w(e))
				}, [
					Y("header", null, [Y("div", null, [Y("span", null, V(e.provider_label || E(e.provider)), 1), Y("strong", null, V(D(e)), 1)]), Y("b", null, V(T(e.status)), 1)]),
					Y("div", LO, [Y("span", { style: R({ width: `${w(e) === "loaded" ? 100 : C(e.progress)}%` }) }, null, 4)]),
					Y("footer", null, [Y("span", null, V(e.message || T(e.status)), 1), Y("b", null, V(Math.round(w(e) === "loaded" ? 100 : C(e.progress))) + "%", 1)])
				], 2))), 128))])) : Z("", !0),
				a.value.length ? (q(), J("div", RO, [n[6] ||= Y("strong", null, "What needs attention", -1), (q(!0), J(K, null, G(a.value, (e) => (q(), J("span", { key: e }, V(e), 1))), 128))])) : Z("", !0),
				Y("footer", null, [Y("span", null, V(m.value ? a.value.length ? "Model changes finished with an issue." : "Your selected models are ready." : "Tater will keep working in the background."), 1), Y("button", {
					class: "tv-button primary",
					type: "button",
					onClick: n[1] ||= (e) => r("close")
				}, V(m.value ? "Done" : "Continue in background"), 1)])
			], 2)]),
			_: 1
		}, 8, ["open"]));
	}
}), BO = { class: "tm-media-header" }, VO = { class: "tm-media-title" }, HO = { class: "tv-eyebrow" }, UO = { class: "tm-speech-status-chip" }, WO = { class: "tm-media-section" }, GO = { class: "tm-speech-section-heading compact" }, KO = ["aria-label"], qO = ["onClick"], JO = {
	key: 0,
	class: "tm-media-section tm-media-provider-section"
}, YO = { class: "tm-speech-section-heading compact" }, XO = ["aria-label"], ZO = ["onClick"], QO = { class: "tm-media-config-panel" }, $O = {
	key: 0,
	class: "tm-field-grid"
}, ek = { class: "tm-field" }, tk = { class: "tm-field" }, nk = {
	key: 1,
	class: "tm-field tm-field-wide"
}, rk = { class: "tm-field-label" }, ik = ["value"], ak = ["value"], ok = { key: 0 }, sk = { key: 1 }, ck = {
	key: 2,
	class: "tm-field tm-field-wide"
}, lk = {
	key: 1,
	class: "tm-media-route-summary"
}, uk = {
	key: 2,
	class: "tm-media-limits"
}, dk = {
	key: 3,
	class: "tm-speech-advanced tm-media-advanced"
}, fk = { class: "tm-field-grid" }, pk = { class: "tm-field" }, mk = { class: "tm-field" }, hk = /* @__PURE__ */ sr({
	__name: "MediaModelCard",
	props: {
		kind: {},
		draft: {},
		localModels: {}
	},
	emits: ["dirty"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = Q(() => n.kind === "vision" ? "Image Understanding" : n.kind === "audio" ? "Audio Understanding" : "Video Understanding"), a = Q(() => n.kind === "vision" ? "vision" : `${n.kind}_understanding`), o = Q(() => `${a.value}_mode`), s = Q(() => `${a.value}_provider`), c = Q(() => `${a.value}_model`), l = Q(() => `${a.value}_api_base`), u = Q(() => `${a.value}_api_key`), d = Q(() => `${a.value}_max_seconds`), f = Q(() => String(n.draft[o.value] || (n.kind === "vision" ? "api" : "base"))), p = Q(() => String(n.draft[s.value] || (n.kind === "vision" ? "openai_compatible" : "llama_cpp"))), m = Q(() => Array.isArray(n.localModels?.models) ? n.localModels.models : []), h = Q(() => m.value.filter((e) => {
			if (String(e.provider || "") !== p.value || e.is_speculative_draft === !0) return !1;
			let t = n.kind === "vision" ? "supports_vision" : n.kind === "audio" ? "supports_audio" : "supports_video";
			return ![
				"supports_vision",
				"supports_audio",
				"supports_video"
			].some((t) => !!e[t]) || !!e[t];
		})), g = [
			{
				value: "openai_compatible",
				label: "OpenAI-Compatible API",
				mark: "API",
				short: "Use a hosted or LAN multimodal endpoint."
			},
			{
				value: "hf_transformers",
				label: "Hugging Face Transformers",
				mark: "HF",
				short: "Run a downloaded Transformers model locally."
			},
			{
				value: "llama_cpp",
				label: "llama.cpp Local",
				mark: "GGUF",
				short: "Run a downloaded quantized model in Tater."
			},
			{
				value: "llama_cpp_remote",
				label: "llama.cpp Remote",
				mark: "LAN",
				short: "Connect to a llama.cpp server on another machine."
			},
			{
				value: "mlx_lm",
				label: "MLX LM",
				mark: "MLX",
				short: "Use Apple Silicon optimized local inference."
			}
		], _ = Q(() => [
			...n.kind === "vision" ? [{
				value: "api",
				label: "API",
				mark: "API",
				short: "Use a dedicated API model for every image."
			}] : [],
			{
				value: "base",
				label: "Same as Base",
				mark: "↔",
				short: "Reuse the main LLM when it supports this media."
			},
			{
				value: "auto",
				label: "Auto",
				mark: "A",
				short: "Let Tater choose the best available capable route."
			},
			{
				value: "dedicated",
				label: "Dedicated model",
				mark: "+",
				short: "Choose a separate model used only for this media."
			}
		]), v = Q(() => _.value.find((e) => e.value === f.value) || _.value[0]), y = Q(() => g.find((e) => e.value === p.value) || g[0]), b = Q(() => n.kind === "vision" ? {
			eyebrow: "Visual intelligence",
			title: "What should inspect images?",
			mark: "VIS",
			short: "Still images, camera frames, screenshots, and image-enabled tools."
		} : n.kind === "audio" ? {
			eyebrow: "Audio intelligence",
			title: "What should understand sound?",
			mark: "AUD",
			short: "Speech, music, ambience, and other bounded audio clips."
		} : {
			eyebrow: "Video intelligence",
			title: "What should inspect video?",
			mark: "VID",
			short: "Short clips analyzed as frames, actions, and scene changes."
		});
		function x() {
			r("dirty");
		}
		function S(e) {
			n.draft[o.value] = e, x();
		}
		function C(e) {
			n.draft[s.value] = e, x();
		}
		function w() {
			return [
				"hf_transformers",
				"llama_cpp",
				"mlx_lm"
			].includes(p.value);
		}
		function T() {
			return ["openai_compatible", "llama_cpp_remote"].includes(p.value);
		}
		function E() {
			return m.value.some((e) => String(e.provider || "") === p.value && String(e.model || "") === String(n.draft[c.value] || ""));
		}
		return (t, n) => (q(), J("article", { class: B(["tm-form-card tm-media-card", `tm-media-${e.kind}`]) }, [
			Y("header", BO, [Y("div", VO, [Y("i", null, V(b.value.mark), 1), Y("div", null, [
				Y("span", HO, V(b.value.eyebrow), 1),
				Y("h3", null, V(i.value), 1),
				Y("p", null, V(b.value.short), 1)
			])]), Y("span", UO, V(v.value.label), 1)]),
			Y("section", WO, [Y("div", GO, [Y("div", null, [Y("h3", null, V(b.value.title), 1), n[8] ||= Y("p", null, "Pick the routing behavior first. Detailed settings appear only when they are needed.", -1)]), n[9] ||= Y("span", null, "Route", -1)]), Y("div", {
				class: "tm-choice-grid tm-media-mode-grid",
				role: "group",
				"aria-label": `${i.value} mode`
			}, [(q(!0), J(K, null, G(_.value, (e) => (q(), J("button", {
				key: e.value,
				type: "button",
				class: B({ active: f.value === e.value }),
				onClick: (t) => S(e.value)
			}, [
				Y("i", null, V(e.mark), 1),
				Y("span", null, [Y("strong", null, V(e.label), 1), Y("small", null, V(e.short), 1)]),
				n[10] ||= Y("b", null, "✓", -1)
			], 10, qO))), 128))], 8, KO)]),
			f.value === "dedicated" || f.value === "api" ? (q(), J("section", JO, [
				Y("div", YO, [n[11] ||= Y("div", null, [Y("h3", null, "Choose a provider"), Y("p", null, "Only compatible installed models appear in the local model picker.")], -1), Y("span", null, V(y.value.label), 1)]),
				Y("div", {
					class: "tm-choice-grid tm-media-provider-grid",
					role: "group",
					"aria-label": `${i.value} provider`
				}, [(q(), J(K, null, G(g, (e) => Y("button", {
					key: e.value,
					type: "button",
					class: B({ active: p.value === e.value }),
					onClick: (t) => C(e.value)
				}, [
					Y("i", null, V(e.mark), 1),
					Y("span", null, [Y("strong", null, V(e.label), 1), Y("small", null, V(e.short), 1)]),
					n[12] ||= Y("b", null, "✓", -1)
				], 10, ZO)), 64))], 8, XO),
				Y("div", QO, [T() ? (q(), J("div", $O, [Y("label", ek, [
					n[13] ||= Y("span", { class: "tm-field-label" }, "API base URL", -1),
					W(Y("input", {
						"onUpdate:modelValue": n[0] ||= (t) => e.draft[l.value] = t,
						type: "url",
						placeholder: "http://127.0.0.1:1234",
						onInput: x
					}, null, 544), [[$, e.draft[l.value]]]),
					n[14] ||= Y("small", null, "Base URL for the compatible inference server.", -1)
				]), Y("label", tk, [
					n[15] ||= Y("span", { class: "tm-field-label" }, "API key", -1),
					W(Y("input", {
						"onUpdate:modelValue": n[1] ||= (t) => e.draft[u.value] = t,
						type: "password",
						autocomplete: "off",
						placeholder: "Optional",
						onInput: x
					}, null, 544), [[$, e.draft[u.value]]]),
					n[16] ||= Y("small", null, "Stored locally and sent only to this endpoint.", -1)
				])])) : Z("", !0), w() ? (q(), J("label", nk, [
					Y("span", rk, "Downloaded " + V(e.kind) + " model", 1),
					W(Y("select", {
						"onUpdate:modelValue": n[2] ||= (t) => e.draft[c.value] = t,
						onChange: x
					}, [
						n[17] ||= Y("option", { value: "" }, "Select a downloaded model", -1),
						e.draft[c.value] && !E() ? (q(), J("option", {
							key: 0,
							value: e.draft[c.value]
						}, "Current: " + V(e.draft[c.value]), 9, ik)) : Z("", !0),
						(q(!0), J(K, null, G(h.value, (e) => (q(), J("option", {
							key: String(e.model),
							value: e.model
						}, V(e.model), 9, ak))), 128))
					], 544), [[ds, e.draft[c.value]]]),
					h.value.length ? (q(), J("small", sk, V(h.value.length) + " compatible downloaded model" + V(h.value.length === 1 ? "" : "s") + " available.", 1)) : (q(), J("small", ok, "No compatible downloaded models were detected. Add one from the Hugging Face tab."))
				])) : (q(), J("label", ck, [
					n[18] ||= Y("span", { class: "tm-field-label" }, "Model name or alias", -1),
					W(Y("input", {
						"onUpdate:modelValue": n[3] ||= (t) => e.draft[c.value] = t,
						type: "text",
						placeholder: "Model id or server alias",
						onInput: x
					}, null, 544), [[$, e.draft[c.value]]]),
					n[19] ||= Y("small", null, "Use the exact model name exposed by the selected server.", -1)
				]))])
			])) : (q(), J("div", lk, [Y("i", null, V(v.value.mark), 1), Y("span", null, [Y("strong", null, V(v.value.label), 1), Y("small", null, V(v.value.short) + " No separate provider configuration is required.", 1)])])),
			e.kind === "vision" ? Z("", !0) : (q(), J("section", uk, [n[21] ||= Y("div", null, [Y("strong", null, "Maximum clip length"), Y("small", null, "Longer clips need more context and take longer to process.")], -1), Y("label", null, [W(Y("input", {
				"onUpdate:modelValue": n[4] ||= (t) => e.draft[d.value] = t,
				type: "range",
				min: "1",
				max: "3600",
				step: "1",
				onInput: x
			}, null, 544), [[$, e.draft[d.value]]]), Y("span", null, [W(Y("input", {
				"onUpdate:modelValue": n[5] ||= (t) => e.draft[d.value] = t,
				type: "number",
				min: "1",
				max: "3600",
				step: "1",
				onInput: x
			}, null, 544), [[$, e.draft[d.value]]]), n[20] ||= X(" sec", -1)])])])),
			e.kind === "vision" ? (q(), J("details", dk, [n[26] ||= Y("summary", null, [Y("span", null, [Y("strong", null, "llama.cpp performance"), Y("small", null, "Context allocation and optional dedicated slot")]), Y("b", null, "⌄")], -1), Y("div", fk, [Y("label", pk, [
				n[22] ||= Y("span", { class: "tm-field-label" }, "Multimodal context", -1),
				W(Y("input", {
					"onUpdate:modelValue": n[6] ||= (t) => e.draft.hydra_llama_cpp_vision_context_tokens = t,
					type: "number",
					min: "256",
					max: "262144",
					step: "256",
					onInput: x
				}, null, 544), [[$, e.draft.hydra_llama_cpp_vision_context_tokens]]),
				n[23] ||= Y("small", null, "Maximum tokens reserved for image requests.", -1)
			]), Y("label", mk, [
				n[24] ||= Y("span", { class: "tm-field-label" }, "Prompt cache slot", -1),
				W(Y("input", {
					"onUpdate:modelValue": n[7] ||= (t) => e.draft.hydra_llama_cpp_vision_slot = t,
					type: "number",
					min: "0",
					max: "31",
					placeholder: "Auto",
					onInput: x
				}, null, 544), [[$, e.draft.hydra_llama_cpp_vision_slot]]),
				n[25] ||= Y("small", null, "Leave blank for automatic slot selection.", -1)
			])])])) : Z("", !0)
		], 2));
	}
}), gk = { class: "tm-field-sections" }, _k = { key: 0 }, vk = { key: 0 }, yk = { class: "tm-field-grid" }, bk = {
	key: 0,
	class: "tm-field-section-break"
}, xk = { key: 0 }, Sk = {
	key: 1,
	class: "tm-field tm-field-wide tm-led-preview"
}, Ck = { class: "tm-field-label" }, wk = { class: "tm-led-preview-grid" }, Tk = {
	key: 2,
	class: "tm-field tm-field-wide tm-runtime-table-field"
}, Ek = { class: "tm-field-label" }, Dk = { class: "tm-table-wrap" }, Ok = { key: 0 }, kk = ["colspan"], Ak = { key: 0 }, jk = { key: 0 }, Mk = [
	"checked",
	"disabled",
	"onChange"
], Nk = { class: "tm-field-label" }, Pk = [
	"value",
	"disabled",
	"onChange"
], Fk = ["label"], Ik = ["value"], Lk = ["value"], Rk = [
	"value",
	"placeholder",
	"readonly",
	"onInput"
], zk = [
	"type",
	"value",
	"placeholder",
	"min",
	"max",
	"step",
	"readonly",
	"onInput"
], Bk = { key: 3 }, Vk = /* @__PURE__ */ sr({
	__name: "ModelFields",
	props: {
		sections: { default: () => [] },
		values: {},
		disabled: {
			type: Boolean,
			default: !1
		}
	},
	emits: ["change"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = Q(() => Array.isArray(n.sections) ? n.sections : []);
		function a(e) {
			return Array.isArray(e.fields) ? e.fields : [];
		}
		function o(e) {
			return String(e.key || e.id || "").trim();
		}
		function s(e) {
			let t = String(e.type || "text").trim().toLowerCase();
			return t === "toggle" ? "checkbox" : t === "readonly" ? "text" : t;
		}
		function c(e) {
			let t = o(e);
			return Object.prototype.hasOwnProperty.call(n.values, t) ? n.values[t] : e.value ?? e.default ?? (s(e) !== "checkbox" && "");
		}
		function l(e) {
			let t = e.dependent_options && typeof e.dependent_options == "object" ? e.dependent_options : null;
			if (t) {
				let e = String(t.source_key || ""), r = String(n.values[e] ?? ""), i = (t.options_by_source && typeof t.options_by_source == "object" ? t.options_by_source : {})[r];
				if (Array.isArray(i)) return i;
				if (Array.isArray(t.default_options)) return t.default_options;
			}
			return Array.isArray(e.options) ? e.options : [];
		}
		function u(e) {
			if (e && typeof e == "object") {
				let t = e;
				return String(t.value ?? t.id ?? t.key ?? "");
			}
			return String(e ?? "");
		}
		function d(e) {
			if (e && typeof e == "object") {
				let t = e;
				return String(t.label ?? t.name ?? t.value ?? t.id ?? "");
			}
			return String(e ?? "");
		}
		function f(e) {
			if (!e || typeof e != "object") return [];
			let t = e.options;
			return Array.isArray(t) ? t : [];
		}
		function p(e) {
			let t = e.show_when && typeof e.show_when == "object" ? e.show_when : null;
			if (!t) return !0;
			let r = n.values[String(t.source_key || "")];
			return Array.isArray(t.any_of) ? t.any_of.map(String).includes(String(r ?? "")) : !Object.prototype.hasOwnProperty.call(t, "equals") || String(r ?? "") === String(t.equals ?? "");
		}
		function m(e) {
			let t = String(e.type || "").trim().toLowerCase();
			return n.disabled || !!(e.disabled || e.read_only || e.readonly || [
				"table",
				"readonly",
				"section",
				"led_preview"
			].includes(t));
		}
		function h(e, t) {
			let n = t.target, i = s(e) === "checkbox" && n instanceof HTMLInputElement ? n.checked : n.value;
			r("change", o(e), i);
		}
		function g(e) {
			return Array.isArray(e.columns) ? e.columns : [];
		}
		function _(e) {
			return Array.isArray(e.rows) ? e.rows : [];
		}
		function v(e) {
			return String(n.values[String(e.animation_key || "")] || "pulse").trim().toLowerCase().replace(/[^a-z0-9_-]+/g, "_") || "pulse";
		}
		function y(e) {
			return v(e).replace(/[_-]+/g, " ").replace(/\b\w/g, (e) => e.toUpperCase());
		}
		function b() {
			let e = Math.max(0, Math.min(100, Number(n.values.led_brightness ?? 100)));
			return {
				"--tm-led-color": String(n.values.led_color || "#ff9b45"),
				"--tm-led-strength": String(.42 + e / 100 * .58)
			};
		}
		return (e, t) => (q(), J("section", gk, [(q(!0), J(K, null, G(i.value, (e, n) => (q(), J("article", {
			key: String(e.label || n),
			class: "tm-form-card"
		}, [e.label || e.description ? (q(), J("header", _k, [Y("h3", null, V(e.label || "Settings"), 1), e.description ? (q(), J("p", vk, V(e.description), 1)) : Z("", !0)])) : Z("", !0), Y("div", yk, [(q(!0), J(K, null, G(a(e), (e) => (q(), J(K, { key: o(e) }, [p(e) && s(e) === "section" ? (q(), J("div", bk, [Y("strong", null, V(e.label || "Settings"), 1), e.description ? (q(), J("small", xk, V(e.description), 1)) : Z("", !0)])) : p(e) && s(e) === "led_preview" ? (q(), J("div", Sk, [Y("span", Ck, [X(V(e.label || "LED Preview") + " ", 1), t[0] ||= Y("small", null, "Live examples", -1)]), Y("div", wk, [(q(!0), J(K, null, G(e.states || [], (e) => (q(), J("article", {
			key: String(e.label),
			class: "tm-led-preview-card",
			style: R(b())
		}, [Y("div", {
			class: B(["tm-led-stage", `animation-${v(e)}`]),
			"aria-hidden": "true"
		}, [
			t[1] ||= Y("span", { class: "tm-led-halo" }, null, -1),
			(q(), J(K, null, G(12, (e) => Y("i", {
				key: e,
				class: "tm-led-dot",
				style: R(`--tm-led-i:${e - 1}`)
			}, null, 4)), 64)),
			t[2] ||= Y("span", { class: "tm-led-core" }, null, -1)
		], 2), Y("div", null, [Y("strong", null, V(e.label), 1), Y("small", null, V(y(e)), 1)])], 4))), 128))])])) : p(e) && s(e) === "table" ? (q(), J("div", Tk, [
			Y("span", Ek, V(e.label), 1),
			Y("div", Dk, [Y("table", null, [Y("thead", null, [Y("tr", null, [(q(!0), J(K, null, G(g(e), (e) => (q(), J("th", { key: String(e.key) }, V(e.label || e.key), 1))), 128))])]), Y("tbody", null, [(q(!0), J(K, null, G(_(e), (t, n) => (q(), J("tr", { key: n }, [(q(!0), J(K, null, G(g(e), (e) => (q(), J("td", { key: String(e.key) }, V(t[String(e.key)] ?? "—"), 1))), 128))]))), 128)), _(e).length ? Z("", !0) : (q(), J("tr", Ok, [Y("td", { colspan: Math.max(1, g(e).length) }, "No results yet.", 8, kk)]))])])]),
			e.description ? (q(), J("small", Ak, V(e.description), 1)) : Z("", !0)
		])) : p(e) && s(e) === "checkbox" ? (q(), J("label", {
			key: 3,
			class: B(["tm-field tm-toggle-field", { "tm-field-wide": e.full_width }])
		}, [Y("span", null, [Y("strong", null, V(e.label || o(e)), 1), e.description ? (q(), J("small", jk, V(e.description), 1)) : Z("", !0)]), Y("input", {
			type: "checkbox",
			checked: !!c(e),
			disabled: m(e),
			onChange: (t) => h(e, t)
		}, null, 40, Mk)], 2)) : p(e) ? (q(), J("label", {
			key: 4,
			class: B(["tm-field", { "tm-field-wide": e.full_width || s(e) === "textarea" }])
		}, [
			Y("span", Nk, V(e.label || o(e)), 1),
			s(e) === "select" ? (q(), J("select", {
				key: 0,
				value: String(c(e) ?? ""),
				disabled: m(e),
				onChange: (t) => h(e, t)
			}, [(q(!0), J(K, null, G(l(e), (e, t) => (q(), J(K, { key: u(e) || `${d(e)}:${t}` }, [f(e).length ? (q(), J("optgroup", {
				key: 0,
				label: d(e)
			}, [(q(!0), J(K, null, G(f(e), (e) => (q(), J("option", {
				key: u(e),
				value: u(e)
			}, V(d(e)), 9, Ik))), 128))], 8, Fk)) : (q(), J("option", {
				key: 1,
				value: u(e)
			}, V(d(e)), 9, Lk))], 64))), 128))], 40, Pk)) : s(e) === "textarea" ? (q(), J("textarea", {
				key: 1,
				value: String(c(e) ?? ""),
				placeholder: String(e.placeholder || ""),
				readonly: m(e),
				onInput: (t) => h(e, t)
			}, null, 40, Rk)) : (q(), J("input", {
				key: 2,
				type: s(e) === "number" ? "number" : s(e) === "password" ? "password" : s(e) === "time" ? "time" : s(e) === "color" ? "color" : "text",
				value: String(c(e) ?? ""),
				placeholder: String(e.placeholder || ""),
				min: e.min,
				max: e.max,
				step: e.step,
				readonly: m(e),
				onInput: (t) => h(e, t)
			}, null, 40, zk)),
			e.description ? (q(), J("small", Bk, V(e.description), 1)) : Z("", !0)
		], 2)) : Z("", !0)], 64))), 128))])]))), 128))]));
	}
}), Hk = { class: "tm-stack tm-tts-profile" }, Uk = { class: "tm-form-card tm-tts-engine-card" }, Wk = { class: "tm-speech-status-chip" }, Gk = {
	key: 0,
	class: "tv-notice error"
}, Kk = ["aria-label"], qk = ["aria-pressed", "onClick"], Jk = {
	key: 0,
	class: "tm-form-card tm-speech-inherit-card"
}, Yk = {
	key: 1,
	class: "tm-form-card tm-tts-config-card"
}, Xk = { class: "tv-eyebrow" }, Zk = { class: "tm-speech-status-chip" }, Qk = {
	key: 0,
	class: "tm-speech-connection-panel"
}, $k = { class: "tm-speech-section-heading compact" }, eA = { class: "tm-field-grid" }, tA = { class: "tm-field tm-field-wide" }, nA = { class: "tm-field" }, rA = { class: "tm-field tm-field-wide" }, iA = { class: "tm-field tm-field-wide" }, aA = { class: "tm-field tm-field-wide" }, oA = { class: "tm-field" }, sA = {
	key: 0,
	class: "tm-speech-discovery"
}, cA = ["disabled"], lA = { class: "tm-speech-voice-panel" }, uA = { class: "tm-speech-section-heading compact" }, dA = { class: "tm-field-grid" }, fA = {
	key: 0,
	class: "tm-field"
}, pA = ["value"], mA = ["value"], hA = {
	key: 1,
	class: "tm-field"
}, gA = {
	key: 2,
	class: "tm-field"
}, _A = ["value"], vA = ["value"], yA = {
	key: 3,
	class: "tm-field"
}, bA = {
	key: 4,
	class: "tm-field"
}, xA = {
	key: 5,
	class: "tm-field"
}, SA = {
	key: 1,
	class: "tm-speech-advanced"
}, CA = { class: "tm-field-grid" }, wA = { class: "tm-field" }, TA = { class: "tm-field" }, EA = { class: "tm-field" }, DA = { class: "tm-field" }, OA = { class: "tm-field" }, kA = { class: "tm-field" }, AA = { class: "tm-field" }, jA = {
	key: 0,
	class: "tm-field tm-toggle-field tm-field-wide"
}, MA = {
	key: 2,
	class: "tm-form-card tm-speech-clone-card"
}, NA = { class: "tm-field-grid" }, PA = { class: "tm-field tm-field-wide" }, FA = { class: "tm-field" }, IA = { class: "tm-field" }, LA = { class: "tm-field tm-field-wide tm-speech-file-field" }, RA = ["disabled"], zA = {
	key: 0,
	class: "tm-inline-actions tm-secondary-row"
}, BA = ["disabled"], VA = /* @__PURE__ */ sr({
	__name: "TtsProfile",
	props: {
		scope: {},
		draft: {},
		ui: {},
		endpoints: {}
	},
	emits: ["dirty", "notify"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ U(""), a = /* @__PURE__ */ U(""), o = /* @__PURE__ */ U([]), s = /* @__PURE__ */ U([]), c = {
			same_as_direct: {
				mark: "↔",
				short: "Use the complete Reply Voice profile.",
				kind: "Shared"
			},
			wyoming: {
				mark: "WY",
				short: "Connect to a Wyoming voice service.",
				kind: "Network"
			},
			openai_compatible: {
				mark: "API",
				short: "Use an OpenAI-style speech endpoint.",
				kind: "API"
			},
			chatterbox: {
				mark: "CB",
				short: "Expressive speech with optional tuning.",
				kind: "Server"
			},
			kokoro: {
				mark: "KO",
				short: "Fast local neural speech synthesis.",
				kind: "Local"
			},
			pocket_tts: {
				mark: "PT",
				short: "Small local voice model with presets.",
				kind: "Local"
			},
			piper: {
				mark: "PI",
				short: "Reliable, lightweight offline voices.",
				kind: "Local"
			},
			qwen3_tts: {
				mark: "Q3",
				short: "Local multilingual speech and voice cloning.",
				kind: "Local clone"
			},
			omnivoice: {
				mark: "OM",
				short: "Local natural speech and voice cloning.",
				kind: "Local clone"
			}
		}, l = Q(() => n.scope === "announcement"), u = Q(() => l.value ? "speech_announcement_" : "speech_"), d = Q(() => `${u.value}tts_backend`), f = Q(() => `${u.value}tts_model`), p = Q(() => `${u.value}tts_voice`), m = Q(() => String(n.draft[d.value] || (l.value ? "same_as_direct" : "wyoming"))), h = Q(() => l.value && ["same_as_direct", "direct"].includes(m.value)), g = Q(() => h.value ? String(n.draft.speech_tts_backend || "wyoming") : m.value), _ = Q(() => {
			if (s.value.length) return s.value;
			let e = n.ui.tts_model_options_by_backend && typeof n.ui.tts_model_options_by_backend == "object" ? n.ui.tts_model_options_by_backend : {};
			return Array.isArray(e[g.value]) ? e[g.value] : [];
		}), v = Q(() => {
			if (o.value.length) return o.value;
			let e = n.ui.tts_voice_options_by_model && typeof n.ui.tts_voice_options_by_model == "object" ? n.ui.tts_voice_options_by_model : {};
			return Array.isArray(e[String(n.draft[f.value] || "")]) ? e[String(n.draft[f.value] || "")] : [];
		}), y = Q(() => Array.isArray(n.ui.tts_backend_options) ? n.ui.tts_backend_options : []), b = Q(() => ee(m.value)), x = Q(() => ee(String(n.draft.speech_tts_backend || "wyoming")).label), S = Q(() => !h.value && [
			"wyoming",
			"openai_compatible",
			"chatterbox"
		].includes(g.value)), C = Q(() => v.value.length > 0 || [
			"wyoming",
			"openai_compatible",
			"chatterbox",
			"kokoro",
			"pocket_tts"
		].includes(g.value)), w = Q(() => ["qwen3_tts", "omnivoice"].includes(g.value)), T = Q(() => g.value === "qwen3_tts" ? "qwen_tts" : "omnivoice_tts"), E = Q(() => `${u.value}${T.value}`), D = Q(() => `${E.value}_clone_audio`), O = Q(() => `${E.value}_clone_text`), k = Q(() => `${E.value}_language`), A = Q(() => `${E.value}_instruct`), j = Q(() => n.draft[D.value] && typeof n.draft[D.value] == "object" ? n.draft[D.value] : {});
		function M(e) {
			return `${u.value}${e}`;
		}
		function N() {
			s.value = [], o.value = [], a.value = "", r("dirty");
		}
		function P(e) {
			return String(e.value ?? e.id ?? e.model ?? e.name ?? "");
		}
		function F(e) {
			return String(e.label ?? e.name ?? e.value ?? e.id ?? "");
		}
		function I(e, t) {
			return e.some((e) => P(e) === String(t ?? ""));
		}
		function ee(e) {
			let t = String(e || ""), n = y.value.find((e) => P(e) === t);
			return {
				...c[t] || {
					mark: t.slice(0, 2).toUpperCase(),
					short: "Speech synthesis provider.",
					kind: "Voice"
				},
				label: F(n || {}) || t || "Voice provider"
			};
		}
		function te(e) {
			m.value !== e && (n.draft[d.value] = e, N());
		}
		async function ne() {
			i.value = "discover", a.value = "";
			try {
				let e = {};
				if (g.value === "wyoming") e = await js(n.endpoints.modelsWyomingVoices, {
					host: n.draft[M("wyoming_tts_host")],
					port: n.draft[M("wyoming_tts_port")],
					current_voice: n.draft[p.value]
				});
				else if (g.value === "openai_compatible") {
					let t = {
						base_url: n.draft[M("openai_tts_base_url")],
						api_key: n.draft[M("openai_tts_api_key")]
					}, [r, i] = await Promise.all([js(n.endpoints.modelsOpenAiModels, t), js(n.endpoints.modelsOpenAiVoices, t)]);
					e = i, s.value = L(r.models || r.options || []);
				} else g.value === "chatterbox" && (e = await js(n.endpoints.modelsChatterboxVoices, {
					base_url: n.draft[M("chatterbox_tts_base_url")],
					voice_mode: n.draft[M("chatterbox_tts_voice_mode")]
				}));
				o.value = L(e.voices || e.options || []), r("notify", `Loaded ${o.value.length} voice option(s).`, "success");
			} catch (e) {
				a.value = e instanceof Error ? e.message : "Voice options could not be loaded.";
			} finally {
				i.value = "";
			}
		}
		function L(e) {
			return (Array.isArray(e) ? e : []).map((e) => typeof e == "object" && e ? e : {
				value: String(e),
				label: String(e)
			});
		}
		async function R(e) {
			let t = e.target, o = t.files?.[0];
			if (!(!o || !w.value)) {
				i.value = "upload", a.value = "";
				try {
					let e = `${n.endpoints.modelsCloneAudio}/${g.value}?scope=${n.scope}`, t = await ks(await fetch(e, {
						method: "POST",
						credentials: "same-origin",
						headers: {
							Accept: "application/json",
							"X-Filename": o.name,
							"Content-Type": o.type || "application/octet-stream"
						},
						body: o
					}));
					n.draft[D.value] = t.audio || {
						configured: !0,
						name: o.name,
						size: o.size
					}, t.clone_text && (n.draft[O.value] = t.clone_text), r("dirty"), r("notify", "Reference audio uploaded and analyzed.", "success");
				} catch (e) {
					a.value = e instanceof Error ? e.message : "Reference audio could not be uploaded.";
				} finally {
					i.value = "", t.value = "";
				}
			}
		}
		async function z() {
			if (!(!w.value || !window.confirm("Remove this reference voice recording?"))) {
				i.value = "delete";
				try {
					let e = `${n.endpoints.modelsCloneAudio}/${g.value}?scope=${n.scope}`, t = await ks(await fetch(e, {
						method: "DELETE",
						credentials: "same-origin",
						headers: { Accept: "application/json" }
					}));
					n.draft[D.value] = t.audio || {}, n.draft[O.value] = "", r("dirty");
				} catch (e) {
					a.value = e instanceof Error ? e.message : "Reference audio could not be removed.";
				} finally {
					i.value = "";
				}
			}
		}
		return (t, n) => (q(), J("section", Hk, [
			Y("article", Uk, [
				Y("header", null, [Y("div", null, [
					n[44] ||= Y("span", { class: "tv-eyebrow" }, "Voice engine", -1),
					Y("h3", null, V(l.value ? "Choose the announcement voice" : "Choose Tater's reply voice"), 1),
					Y("p", null, V(l.value ? "Reuse the Reply Voice profile or select a dedicated engine for announcements." : "Select one engine. Only its matching model and connection settings appear."), 1)
				]), Y("span", Wk, V(b.value.label), 1)]),
				a.value ? (q(), J("div", Gk, V(a.value), 1)) : Z("", !0),
				Y("div", {
					class: "tm-speech-provider-grid tm-tts-provider-grid",
					role: "group",
					"aria-label": `${l.value ? "Announcement" : "Reply"} voice engine`
				}, [(q(!0), J(K, null, G(y.value, (e) => (q(), J("button", {
					key: P(e),
					type: "button",
					class: B({ active: m.value === P(e) }),
					"aria-pressed": m.value === P(e),
					onClick: (t) => te(P(e))
				}, [
					Y("i", null, V(ee(P(e)).mark), 1),
					Y("span", null, [Y("strong", null, V(F(e)), 1), Y("small", null, V(ee(P(e)).short), 1)]),
					n[45] ||= Y("b", { "aria-hidden": "true" }, "✓", -1)
				], 10, qk))), 128))], 8, Kk)
			]),
			h.value ? (q(), J("article", Jk, [
				n[46] ||= Y("i", null, "↔", -1),
				n[47] ||= Y("div", null, [
					Y("span", { class: "tv-eyebrow" }, "Linked profile"),
					Y("h3", null, "Announcements use the complete Reply Voice setup"),
					Y("p", null, "Model, voice, connection, cloning, and tuning changes from Reply Voice are applied automatically.")
				], -1),
				Y("span", null, V(x.value), 1)
			])) : (q(), J("article", Yk, [
				Y("header", null, [Y("div", null, [
					Y("span", Xk, V(b.value.kind) + " setup", 1),
					Y("h3", null, V(b.value.label), 1),
					Y("p", null, V(b.value.short), 1)
				]), Y("span", Zk, V(e.draft[p.value] || e.draft[f.value] || "Choose voice"), 1)]),
				[
					"wyoming",
					"openai_compatible",
					"chatterbox"
				].includes(g.value) ? (q(), J("section", Qk, [
					Y("div", $k, [n[48] ||= Y("div", null, [Y("h3", null, "Connection"), Y("p", null, "Tell Tater where this voice service is running.")], -1), Y("span", null, V(b.value.kind), 1)]),
					Y("div", eA, [g.value === "wyoming" ? (q(), J(K, { key: 0 }, [Y("label", tA, [
						n[49] ||= Y("span", { class: "tm-field-label" }, "Wyoming host", -1),
						W(Y("input", {
							"onUpdate:modelValue": n[0] ||= (t) => e.draft[M("wyoming_tts_host")] = t,
							type: "text",
							placeholder: "127.0.0.1",
							onInput: n[1] ||= (e) => r("dirty")
						}, null, 544), [[$, e.draft[M("wyoming_tts_host")]]]),
						n[50] ||= Y("small", null, "Hostname or IP address of the Wyoming TTS service.", -1)
					]), Y("label", nA, [n[51] ||= Y("span", { class: "tm-field-label" }, "Wyoming port", -1), W(Y("input", {
						"onUpdate:modelValue": n[2] ||= (t) => e.draft[M("wyoming_tts_port")] = t,
						type: "number",
						min: "1",
						max: "65535",
						placeholder: "10200",
						onInput: n[3] ||= (e) => r("dirty")
					}, null, 544), [[$, e.draft[M("wyoming_tts_port")]]])])], 64)) : g.value === "openai_compatible" ? (q(), J(K, { key: 1 }, [Y("label", rA, [
						n[52] ||= Y("span", { class: "tm-field-label" }, "Base URL", -1),
						W(Y("input", {
							"onUpdate:modelValue": n[4] ||= (t) => e.draft[M("openai_tts_base_url")] = t,
							type: "text",
							placeholder: "http://127.0.0.1:8000",
							onInput: n[5] ||= (e) => r("dirty")
						}, null, 544), [[$, e.draft[M("openai_tts_base_url")]]]),
						n[53] ||= Y("small", null, "The root URL for an OpenAI-compatible audio API.", -1)
					]), Y("label", iA, [n[54] ||= Y("span", { class: "tm-field-label" }, "API key", -1), W(Y("input", {
						"onUpdate:modelValue": n[6] ||= (t) => e.draft[M("openai_tts_api_key")] = t,
						type: "password",
						autocomplete: "off",
						placeholder: "Optional",
						onInput: n[7] ||= (e) => r("dirty")
					}, null, 544), [[$, e.draft[M("openai_tts_api_key")]]])])], 64)) : (q(), J(K, { key: 2 }, [Y("label", aA, [
						n[55] ||= Y("span", { class: "tm-field-label" }, "Chatterbox URL", -1),
						W(Y("input", {
							"onUpdate:modelValue": n[8] ||= (t) => e.draft[M("chatterbox_tts_base_url")] = t,
							type: "text",
							placeholder: "http://127.0.0.1:8004",
							onInput: n[9] ||= (e) => r("dirty")
						}, null, 544), [[$, e.draft[M("chatterbox_tts_base_url")]]]),
						n[56] ||= Y("small", null, "The Chatterbox server used for synthesis and voice discovery.", -1)
					]), Y("label", oA, [n[58] ||= Y("span", { class: "tm-field-label" }, "Voice source", -1), W(Y("select", {
						"onUpdate:modelValue": n[10] ||= (t) => e.draft[M("chatterbox_tts_voice_mode")] = t,
						onChange: N
					}, [...n[57] ||= [Y("option", { value: "predefined" }, "Predefined voice", -1), Y("option", { value: "clone" }, "Cloned voice", -1)]], 544), [[ds, e.draft[M("chatterbox_tts_voice_mode")]]])])], 64))]),
					S.value ? (q(), J("div", sA, [n[59] ||= Y("div", null, [Y("i", null, "⌕"), Y("span", null, [Y("strong", null, "Find available voices"), Y("small", null, "Ask the connected service for its current models and voices.")])], -1), Y("button", {
						class: "tv-button",
						type: "button",
						disabled: !!i.value,
						onClick: ne
					}, V(i.value === "discover" ? "Discovering…" : "Discover models & voices"), 9, cA)])) : Z("", !0)
				])) : Z("", !0),
				Y("section", lA, [Y("div", uA, [n[60] ||= Y("div", null, [Y("h3", null, "Model and voice"), Y("p", null, "Choose what generates the audio and how Tater should sound.")], -1), Y("span", null, V(_.value.length || v.value.length ? "Available choices" : "Manual entry"), 1)]), Y("div", dA, [
					_.value.length ? (q(), J("label", fA, [n[62] ||= Y("span", { class: "tm-field-label" }, "Model", -1), W(Y("select", {
						"onUpdate:modelValue": n[11] ||= (t) => e.draft[f.value] = t,
						onChange: N
					}, [
						n[61] ||= Y("option", { value: "" }, "Choose a model", -1),
						e.draft[f.value] && !I(_.value, e.draft[f.value]) ? (q(), J("option", {
							key: 0,
							value: e.draft[f.value]
						}, "Current: " + V(e.draft[f.value]), 9, pA)) : Z("", !0),
						(q(!0), J(K, null, G(_.value, (e) => (q(), J("option", {
							key: P(e),
							value: P(e)
						}, V(F(e)), 9, mA))), 128))
					], 544), [[ds, e.draft[f.value]]])])) : ["wyoming", "chatterbox"].includes(g.value) ? Z("", !0) : (q(), J("label", hA, [n[63] ||= Y("span", { class: "tm-field-label" }, "Model", -1), W(Y("input", {
						"onUpdate:modelValue": n[12] ||= (t) => e.draft[f.value] = t,
						type: "text",
						placeholder: "Model id or alias",
						onInput: n[13] ||= (e) => r("dirty")
					}, null, 544), [[$, e.draft[f.value]]])])),
					C.value && v.value.length ? (q(), J("label", gA, [n[65] ||= Y("span", { class: "tm-field-label" }, "Voice", -1), W(Y("select", {
						"onUpdate:modelValue": n[14] ||= (t) => e.draft[p.value] = t,
						onChange: n[15] ||= (e) => r("dirty")
					}, [
						n[64] ||= Y("option", { value: "" }, "Choose a voice", -1),
						e.draft[p.value] && !I(v.value, e.draft[p.value]) ? (q(), J("option", {
							key: 0,
							value: e.draft[p.value]
						}, "Current: " + V(e.draft[p.value]), 9, _A)) : Z("", !0),
						(q(!0), J(K, null, G(v.value, (e) => (q(), J("option", {
							key: P(e),
							value: P(e)
						}, V(F(e)), 9, vA))), 128))
					], 544), [[ds, e.draft[p.value]]])])) : C.value ? (q(), J("label", yA, [n[66] ||= Y("span", { class: "tm-field-label" }, "Voice", -1), W(Y("input", {
						"onUpdate:modelValue": n[16] ||= (t) => e.draft[p.value] = t,
						type: "text",
						placeholder: "Voice id or name",
						onInput: n[17] ||= (e) => r("dirty")
					}, null, 544), [[$, e.draft[p.value]]])])) : Z("", !0),
					g.value === "kokoro" ? (q(), J("label", bA, [
						n[67] ||= Y("span", { class: "tm-field-label" }, "Output gain", -1),
						W(Y("input", {
							"onUpdate:modelValue": n[18] ||= (t) => e.draft[M("kokoro_output_gain")] = t,
							type: "number",
							step: "0.05",
							min: "0",
							onInput: n[19] ||= (e) => r("dirty")
						}, null, 544), [[$, e.draft[M("kokoro_output_gain")]]]),
						n[68] ||= Y("small", null, "Adjust Kokoro's output volume without changing satellite volume.", -1)
					])) : Z("", !0),
					g.value === "pocket_tts" ? (q(), J("label", xA, [
						n[69] ||= Y("span", { class: "tm-field-label" }, "Output gain", -1),
						W(Y("input", {
							"onUpdate:modelValue": n[20] ||= (t) => e.draft[M("pocket_tts_output_gain")] = t,
							type: "number",
							step: "0.05",
							min: "0",
							onInput: n[21] ||= (e) => r("dirty")
						}, null, 544), [[$, e.draft[M("pocket_tts_output_gain")]]]),
						n[70] ||= Y("small", null, "Adjust Pocket TTS output volume.", -1)
					])) : Z("", !0)
				])]),
				g.value === "chatterbox" ? (q(), J("details", SA, [n[79] ||= Y("summary", null, [Y("span", null, [Y("strong", null, "Fine-tune Chatterbox"), Y("small", null, "Optional expression, pacing, and generation controls")]), Y("b", null, "Advanced")], -1), Y("div", CA, [
					Y("label", wA, [n[71] ||= Y("span", { class: "tm-field-label" }, "Language", -1), W(Y("input", {
						"onUpdate:modelValue": n[22] ||= (t) => e.draft[M("chatterbox_tts_language")] = t,
						type: "text",
						placeholder: "Auto",
						onInput: n[23] ||= (e) => r("dirty")
					}, null, 544), [[$, e.draft[M("chatterbox_tts_language")]]])]),
					Y("label", TA, [n[72] ||= Y("span", { class: "tm-field-label" }, "Chunk size", -1), W(Y("input", {
						"onUpdate:modelValue": n[24] ||= (t) => e.draft[M("chatterbox_tts_chunk_size")] = t,
						type: "number",
						min: "1",
						onInput: n[25] ||= (e) => r("dirty")
					}, null, 544), [[$, e.draft[M("chatterbox_tts_chunk_size")]]])]),
					Y("label", EA, [n[73] ||= Y("span", { class: "tm-field-label" }, "Temperature", -1), W(Y("input", {
						"onUpdate:modelValue": n[26] ||= (t) => e.draft[M("chatterbox_tts_temperature")] = t,
						type: "number",
						min: "0",
						step: "0.05",
						onInput: n[27] ||= (e) => r("dirty")
					}, null, 544), [[$, e.draft[M("chatterbox_tts_temperature")]]])]),
					Y("label", DA, [n[74] ||= Y("span", { class: "tm-field-label" }, "Exaggeration", -1), W(Y("input", {
						"onUpdate:modelValue": n[28] ||= (t) => e.draft[M("chatterbox_tts_exaggeration")] = t,
						type: "number",
						min: "0",
						step: "0.05",
						onInput: n[29] ||= (e) => r("dirty")
					}, null, 544), [[$, e.draft[M("chatterbox_tts_exaggeration")]]])]),
					Y("label", OA, [n[75] ||= Y("span", { class: "tm-field-label" }, "CFG weight", -1), W(Y("input", {
						"onUpdate:modelValue": n[30] ||= (t) => e.draft[M("chatterbox_tts_cfg_weight")] = t,
						type: "number",
						min: "0",
						step: "0.05",
						onInput: n[31] ||= (e) => r("dirty")
					}, null, 544), [[$, e.draft[M("chatterbox_tts_cfg_weight")]]])]),
					Y("label", kA, [n[76] ||= Y("span", { class: "tm-field-label" }, "Seed", -1), W(Y("input", {
						"onUpdate:modelValue": n[32] ||= (t) => e.draft[M("chatterbox_tts_seed")] = t,
						type: "number",
						onInput: n[33] ||= (e) => r("dirty")
					}, null, 544), [[$, e.draft[M("chatterbox_tts_seed")]]])]),
					Y("label", AA, [n[77] ||= Y("span", { class: "tm-field-label" }, "Speed", -1), W(Y("input", {
						"onUpdate:modelValue": n[34] ||= (t) => e.draft[M("chatterbox_tts_speed_factor")] = t,
						type: "number",
						min: "0.1",
						step: "0.05",
						onInput: n[35] ||= (e) => r("dirty")
					}, null, 544), [[$, e.draft[M("chatterbox_tts_speed_factor")]]])]),
					l.value ? Z("", !0) : (q(), J("label", jA, [n[78] ||= Y("span", null, [Y("strong", null, "Satellite streaming"), Y("small", null, "Start playback while the rest of the reply is still being generated.")], -1), W(Y("input", {
						"onUpdate:modelValue": n[36] ||= (t) => e.draft.speech_chatterbox_tts_streaming_enabled = t,
						type: "checkbox",
						onChange: n[37] ||= (e) => r("dirty")
					}, null, 544), [[cs, e.draft.speech_chatterbox_tts_streaming_enabled]])]))
				])])) : Z("", !0)
			])),
			w.value && !h.value ? (q(), J("article", MA, [
				Y("header", null, [Y("div", null, [
					n[80] ||= Y("span", { class: "tv-eyebrow" }, "Personal voice", -1),
					n[81] ||= Y("h3", null, "Reference voice", -1),
					Y("p", null, "Upload a clean recording and transcript so " + V(b.value.label) + " can reproduce that voice.", 1)
				]), Y("span", { class: B(["tm-speech-status-chip", { ready: j.value.configured }]) }, V(j.value.configured ? "Reference ready" : "Not configured"), 3)]),
				Y("div", NA, [
					Y("label", PA, [
						n[82] ||= Y("span", { class: "tm-field-label" }, "Reference transcript", -1),
						W(Y("textarea", {
							"onUpdate:modelValue": n[38] ||= (t) => e.draft[O.value] = t,
							placeholder: "Enter exactly what is spoken in the reference recording.",
							onInput: n[39] ||= (e) => r("dirty")
						}, null, 544), [[$, e.draft[O.value]]]),
						n[83] ||= Y("small", null, "A matching transcript produces a more accurate cloned voice.", -1)
					]),
					Y("label", FA, [n[84] ||= Y("span", { class: "tm-field-label" }, "Language", -1), W(Y("input", {
						"onUpdate:modelValue": n[40] ||= (t) => e.draft[k.value] = t,
						type: "text",
						placeholder: "English",
						onInput: n[41] ||= (e) => r("dirty")
					}, null, 544), [[$, e.draft[k.value]]])]),
					Y("label", IA, [n[85] ||= Y("span", { class: "tm-field-label" }, "Voice instruction", -1), W(Y("input", {
						"onUpdate:modelValue": n[42] ||= (t) => e.draft[A.value] = t,
						type: "text",
						placeholder: "Optional style or delivery guidance",
						onInput: n[43] ||= (e) => r("dirty")
					}, null, 544), [[$, e.draft[A.value]]])]),
					Y("label", LA, [
						n[86] ||= Y("span", { class: "tm-field-label" }, "Reference audio", -1),
						Y("input", {
							type: "file",
							accept: "audio/*",
							disabled: !!i.value,
							onChange: R
						}, null, 40, RA),
						Y("small", null, V(j.value.configured ? `${j.value.name} · ${Number(j.value.size || 0).toLocaleString()} bytes` : "Use a short, clear recording with little background noise."), 1)
					])
				]),
				j.value.configured ? (q(), J("div", zA, [Y("button", {
					class: "tv-button danger",
					type: "button",
					disabled: !!i.value,
					onClick: z
				}, V(i.value === "delete" ? "Removing…" : "Remove reference audio"), 9, BA)])) : Z("", !0)
			])) : Z("", !0)
		]));
	}
}), HA = { class: "tm-stack tm-speech-workspace" }, UA = {
	class: "tv-tabs tm-inner-tabs tm-speech-tabs",
	"aria-label": "Speech settings areas"
}, WA = {
	key: 0,
	class: "tv-notice error"
}, GA = { class: "tm-form-card tm-speech-hero" }, KA = { class: "tv-eyebrow" }, qA = { class: "tm-speech-hero-status" }, JA = { class: "tm-speech-status-chip" }, YA = {
	key: 0,
	class: "tm-progress-list"
}, XA = ["value"], ZA = {
	key: 1,
	class: "tm-speech-ready"
}, QA = { class: "tm-form-card tm-speech-picker-card" }, $A = { class: "tm-speech-status-chip" }, ej = {
	class: "tm-speech-provider-grid",
	role: "group",
	"aria-label": "Speech recognition backend"
}, tj = ["aria-pressed", "onClick"], nj = { class: "tm-form-card tm-speech-runtime-card" }, rj = { class: "tm-speech-status-chip" }, ij = {
	key: 0,
	class: "tm-speech-acceleration",
	role: "group",
	"aria-label": "Speech acceleration"
}, aj = ["onClick"], oj = {
	key: 1,
	class: "tm-field-grid tm-speech-connection-fields"
}, sj = { class: "tm-field tm-field-wide" }, cj = { class: "tm-field" }, lj = {
	key: 0,
	class: "tm-speech-runtime-settings"
}, uj = { class: "tm-speech-section-heading" }, dj = { class: "tm-form-card tm-speech-duck-card" }, fj = { class: "tm-speech-status-chip" }, pj = { class: "tm-field-grid" }, mj = { class: "tm-field tm-field-wide" }, hj = { class: "tm-speech-range-field" }, gj = { class: "tm-field" }, _j = { class: "tm-field" }, vj = {
	key: 5,
	class: "tm-form-card tm-preview-card tm-speech-preview-card"
}, yj = { class: "tm-speech-status-chip" }, bj = {
	class: "tm-speech-profile-switch",
	role: "group",
	"aria-label": "Voice profile to preview"
}, xj = { class: "tm-field-grid" }, Sj = { class: "tm-field tm-field-wide" }, Cj = { class: "tm-speech-preview-actions" }, wj = ["disabled"], Tj = {
	key: 0,
	class: "tm-speech-audio-player"
}, Ej = ["src"], Dj = /* @__PURE__ */ sr({
	__name: "SpeechModels",
	props: {
		draft: {},
		speechUi: {},
		announcementUi: {},
		voiceModelUi: {},
		endpoints: {}
	},
	emits: ["dirty", "notify"],
	setup(e, { expose: t, emit: n }) {
		let r = e, i = n, a = /* @__PURE__ */ U("listening"), o = /* @__PURE__ */ U({}), s = /* @__PURE__ */ U("Hello from Tater. This is a voice preview."), c = /* @__PURE__ */ U("direct"), l = /* @__PURE__ */ U(!1), u = /* @__PURE__ */ U(""), d = /* @__PURE__ */ U(null), f = /* @__PURE__ */ U(""), p = null, m = {
			faster_whisper: {
				mark: "FW",
				short: "Fast, accurate local Whisper transcription."
			},
			mlx_whisper: {
				mark: "MLX",
				short: "Whisper optimized for Apple Silicon."
			},
			parakeet_onnx: {
				mark: "PK",
				short: "Efficient local speech recognition with ONNX."
			},
			qwen3_asr_llama_cpp: {
				mark: "Q3",
				short: "Experimental local Qwen3-ASR through llama.cpp."
			},
			wyoming: {
				mark: "WY",
				short: "Use a Wyoming speech service on your network."
			},
			vosk: {
				mark: "VK",
				short: "Lightweight offline recognition for modest hardware."
			}
		}, h = Q(() => Array.isArray(r.voiceModelUi.sections) ? r.voiceModelUi.sections : []), g = Q(() => Array.isArray(r.speechUi.stt_backend_options) ? r.speechUi.stt_backend_options : []), _ = Q(() => Array.isArray(r.speechUi.acceleration_options) ? r.speechUi.acceleration_options : []), v = Q(() => Array.isArray(o.value.items) ? o.value.items : []), y = Q(() => v.value.some((e) => String(e.status || "").toLowerCase() === "error") || Array.isArray(o.value.errors) && o.value.errors.length > 0), b = Q(() => ((!r.draft.esphome_settings || typeof r.draft.esphome_settings != "object") && (r.draft.esphome_settings = {}), r.draft.esphome_settings)), x = Q(() => String(r.draft.speech_stt_backend || "faster_whisper")), S = Q(() => M(g.value, x.value, "Speech recognition")), C = Q(() => M(_.value, r.draft.speech_acceleration, "Auto acceleration")), w = Q(() => x.value !== "wyoming"), T = Q(() => h.value.filter((e) => !String(e.label || "").toLowerCase().includes("faster whisper") || x.value === "faster_whisper")), E = Q(() => M(Array.isArray(r.speechUi.tts_backend_options) ? r.speechUi.tts_backend_options : [], r.draft.speech_tts_backend, "Reply voice")), D = Q(() => {
			let e = String(r.draft.speech_announcement_tts_backend || "same_as_direct");
			return ["same_as_direct", "direct"].includes(e) ? `Uses ${E.value}` : M(Array.isArray(r.announcementUi.tts_backend_options) ? r.announcementUi.tts_backend_options : [], e, "Announcement voice");
		}), O = Q(() => c.value === "direct" ? E.value : D.value), k = Q(() => a.value === "listening" ? {
			eyebrow: "Hear clearly",
			title: "Listening and speech recognition",
			description: "Choose how Tater turns speech into text, then tune only the runtime that is actually in use.",
			status: S.value,
			badge: w.value ? C.value : "Network service"
		} : a.value === "replies" ? {
			eyebrow: "Tater's voice",
			title: "Conversational reply voice",
			description: "Choose the voice engine Tater uses when answering people directly.",
			status: E.value,
			badge: "Direct replies"
		} : a.value === "announcements" ? {
			eyebrow: "Whole-home voice",
			title: "Announcements and proactive speech",
			description: "Reuse the reply voice or give announcements their own sound, then control satellite ducking.",
			status: D.value,
			badge: "Announcements"
		} : {
			eyebrow: "Sound check",
			title: "Preview before you save",
			description: "Generate a sample from the settings currently on screen, including unsaved changes.",
			status: O.value,
			badge: "Live preview"
		});
		function A(e) {
			return String(e.value ?? e.id ?? "");
		}
		function j(e) {
			return String(e.label ?? e.name ?? e.value ?? e.id ?? "");
		}
		function M(e, t, n) {
			return j(e.find((e) => A(e) === String(t || "")) || {}) || n;
		}
		function N(e) {
			return m[A(e)]?.mark || j(e).slice(0, 2).toUpperCase();
		}
		function P(e) {
			return m[A(e)]?.short || "Speech recognition provider.";
		}
		function F() {
			f.value = "", i("dirty");
		}
		function I(e, t) {
			b.value[e] = t, F();
		}
		function ee(e) {
			r.draft.speech_stt_backend = e, F();
		}
		function te(e) {
			r.draft.speech_acceleration = e, F();
		}
		async function ne() {
			try {
				o.value = await As(r.endpoints.modelsSpeechWarmup), o.value.running && L();
			} catch {}
		}
		function L() {
			p !== null && window.clearTimeout(p), p = window.setTimeout(ne, 1500);
		}
		function R() {
			let e = c.value === "announcement", t = e ? "speech_announcement_" : "speech_", n = String(r.draft[`${t}tts_backend`] || ""), i = e && ["same_as_direct", "direct"].includes(n);
			i && (n = String(r.draft.speech_tts_backend || "wyoming"));
			let a = i ? "speech_" : t, o = n === "qwen3_tts" ? "qwen_tts" : n === "omnivoice" ? "omnivoice_tts" : "";
			return {
				text: s.value,
				backend: n,
				model: r.draft[`${a}tts_model`] || r.draft.speech_tts_model || "",
				voice: r.draft[`${a}tts_voice`] || r.draft.speech_tts_voice || "",
				acceleration: r.draft.speech_acceleration || "auto",
				wyoming_host: r.draft[`${a}wyoming_tts_host`] || r.draft.speech_wyoming_tts_host || "",
				wyoming_port: r.draft[`${a}wyoming_tts_port`] || r.draft.speech_wyoming_tts_port || "",
				wyoming_voice: r.draft[`${a}wyoming_tts_voice`] || r.draft.speech_wyoming_tts_voice || "",
				openai_base_url: r.draft[`${a}openai_tts_base_url`] || r.draft.speech_openai_tts_base_url || "",
				openai_api_key: r.draft[`${a}openai_tts_api_key`] || r.draft.speech_openai_tts_api_key || "",
				chatterbox_base_url: r.draft[`${a}chatterbox_tts_base_url`] || r.draft.speech_chatterbox_tts_base_url || "",
				chatterbox_voice_mode: r.draft[`${a}chatterbox_tts_voice_mode`] || r.draft.speech_chatterbox_tts_voice_mode || "",
				chatterbox_chunk_size: r.draft[`${a}chatterbox_tts_chunk_size`] || r.draft.speech_chatterbox_tts_chunk_size || null,
				chatterbox_temperature: r.draft[`${a}chatterbox_tts_temperature`] || r.draft.speech_chatterbox_tts_temperature || null,
				chatterbox_exaggeration: r.draft[`${a}chatterbox_tts_exaggeration`] || r.draft.speech_chatterbox_tts_exaggeration || null,
				chatterbox_cfg_weight: r.draft[`${a}chatterbox_tts_cfg_weight`] || r.draft.speech_chatterbox_tts_cfg_weight || null,
				chatterbox_seed: r.draft[`${a}chatterbox_tts_seed`] || r.draft.speech_chatterbox_tts_seed || null,
				chatterbox_speed_factor: r.draft[`${a}chatterbox_tts_speed_factor`] || r.draft.speech_chatterbox_tts_speed_factor || null,
				chatterbox_language: r.draft[`${a}chatterbox_tts_language`] || r.draft.speech_chatterbox_tts_language || "",
				kokoro_output_gain: r.draft[`${a}kokoro_output_gain`] || r.draft.speech_kokoro_output_gain || null,
				pocket_tts_output_gain: r.draft[`${a}pocket_tts_output_gain`] || r.draft.speech_pocket_tts_output_gain || null,
				clone_text: o && r.draft[`${a}${o}_clone_text`] || "",
				managed_language: o && r.draft[`${a}${o}_language`] || "",
				managed_instruct: o && r.draft[`${a}${o}_instruct`] || ""
			};
		}
		async function z() {
			l.value = !0, f.value = "";
			try {
				let e = await fetch(r.endpoints.modelsSpeechPreview, {
					method: "POST",
					credentials: "same-origin",
					headers: {
						Accept: "audio/wav",
						"Content-Type": "application/json"
					},
					body: JSON.stringify(R())
				});
				e.ok || await ks(e);
				let t = await e.blob();
				u.value && URL.revokeObjectURL(u.value), u.value = URL.createObjectURL(t), await new Promise((e) => window.setTimeout(e, 0)), await d.value?.play().catch(() => {});
			} catch (e) {
				f.value = e instanceof Error ? e.message : "Voice preview failed.", i("notify", f.value, "error");
			} finally {
				l.value = !1;
			}
		}
		return Er(() => {
			ne();
		}), kr(() => {
			p !== null && window.clearTimeout(p), u.value && URL.revokeObjectURL(u.value);
		}), t({ refreshWarmup: ne }), (t, n) => (q(), J("section", HA, [
			Y("nav", UA, [
				Y("button", {
					type: "button",
					class: B({ active: a.value === "listening" }),
					onClick: n[0] ||= (e) => a.value = "listening"
				}, "Listening & STT", 2),
				Y("button", {
					type: "button",
					class: B({ active: a.value === "replies" }),
					onClick: n[1] ||= (e) => a.value = "replies"
				}, "Reply voice", 2),
				Y("button", {
					type: "button",
					class: B({ active: a.value === "announcements" }),
					onClick: n[2] ||= (e) => a.value = "announcements"
				}, "Announcements", 2),
				Y("button", {
					type: "button",
					class: B({ active: a.value === "playback" }),
					onClick: n[3] ||= (e) => a.value = "playback"
				}, "Playback & test", 2)
			]),
			f.value ? (q(), J("div", WA, V(f.value), 1)) : Z("", !0),
			Y("article", GA, [Y("header", null, [Y("div", null, [
				Y("span", KA, V(k.value.eyebrow), 1),
				Y("h3", null, V(k.value.title), 1),
				Y("p", null, V(k.value.description), 1)
			]), Y("div", qA, [Y("span", null, [n[15] ||= Y("i", null, null, -1), X(V(k.value.status), 1)]), Y("b", null, V(k.value.badge), 1)])])]),
			o.value.running || v.value.length ? (q(), J("article", {
				key: 1,
				class: B(["tm-form-card tm-speech-warmup", {
					complete: !o.value.running && !y.value,
					error: y.value
				}])
			}, [Y("header", null, [Y("div", null, [
				n[16] ||= Y("span", { class: "tv-eyebrow" }, "Voice runtime", -1),
				Y("h3", null, V(o.value.running ? "Preparing voice models" : y.value ? "Voice model needs attention" : "Voice models are ready"), 1),
				Y("p", null, V(o.value.running ? "Tater is loading the selected local speech engines in the background." : y.value ? "One or more voice models could not be prepared." : "The latest voice-model preparation finished successfully."), 1)
			]), Y("span", JA, V(o.value.running ? "Working" : y.value ? "Check status" : "Ready"), 1)]), o.value.running ? (q(), J("div", YA, [(q(!0), J(K, null, G(v.value, (e) => (q(), J("div", {
				key: String(e.key || e.model),
				class: "tm-progress-row"
			}, [Y("div", null, [Y("strong", null, V(e.label || e.model || e.backend), 1), Y("span", null, V(e.message || e.status), 1)]), Y("progress", {
				value: Number(e.progress || 0),
				max: "100"
			}, null, 8, XA)]))), 128))])) : (q(), J("div", ZA, [Y("i", null, V(y.value ? "!" : "✓"), 1), Y("span", null, V(v.value.length) + " voice " + V(v.value.length === 1 ? "item" : "items") + " checked", 1)]))], 2)) : Z("", !0),
			a.value === "listening" ? (q(), J(K, { key: 2 }, [
				Y("article", QA, [Y("header", null, [n[17] ||= Y("div", null, [
					Y("span", { class: "tv-eyebrow" }, "Step 1"),
					Y("h3", null, "Choose how Tater listens"),
					Y("p", null, "Only settings for the selected speech-recognition engine appear below.")
				], -1), Y("span", $A, V(S.value), 1)]), Y("div", ej, [(q(!0), J(K, null, G(g.value, (e) => (q(), J("button", {
					key: A(e),
					type: "button",
					class: B({ active: x.value === A(e) }),
					"aria-pressed": x.value === A(e),
					onClick: (t) => ee(A(e))
				}, [
					Y("i", null, V(N(e)), 1),
					Y("span", null, [Y("strong", null, V(j(e)), 1), Y("small", null, V(P(e)), 1)]),
					n[18] ||= Y("b", { "aria-hidden": "true" }, "✓", -1)
				], 10, tj))), 128))])]),
				Y("article", nj, [Y("header", null, [Y("div", null, [
					n[19] ||= Y("span", { class: "tv-eyebrow" }, "Step 2", -1),
					Y("h3", null, V(w.value ? "Choose the processing hardware" : "Connect the Wyoming service"), 1),
					Y("p", null, V(w.value ? "Pick Auto unless you know which accelerator should run speech recognition." : "Enter the host and port of the Wyoming speech-to-text service."), 1)
				]), Y("span", rj, V(w.value ? C.value : "Network"), 1)]), w.value ? (q(), J("div", ij, [(q(!0), J(K, null, G(_.value, (t) => (q(), J("button", {
					key: A(t),
					type: "button",
					class: B({ active: String(e.draft.speech_acceleration || "auto") === A(t) }),
					onClick: (e) => te(A(t))
				}, [n[20] ||= Y("i", null, null, -1), X(V(j(t)), 1)], 10, aj))), 128))])) : (q(), J("div", oj, [Y("label", sj, [
					n[21] ||= Y("span", { class: "tm-field-label" }, "Wyoming host", -1),
					W(Y("input", {
						"onUpdate:modelValue": n[4] ||= (t) => e.draft.speech_wyoming_stt_host = t,
						type: "text",
						placeholder: "127.0.0.1",
						onInput: F
					}, null, 544), [[$, e.draft.speech_wyoming_stt_host]]),
					n[22] ||= Y("small", null, "Hostname or IP address of your Wyoming STT service.", -1)
				]), Y("label", cj, [n[23] ||= Y("span", { class: "tm-field-label" }, "Wyoming port", -1), W(Y("input", {
					"onUpdate:modelValue": n[5] ||= (t) => e.draft.speech_wyoming_stt_port = t,
					type: "number",
					min: "1",
					max: "65535",
					placeholder: "10300",
					onInput: F
				}, null, 544), [[$, e.draft.speech_wyoming_stt_port]])])]))]),
				T.value.length ? (q(), J("section", lj, [Y("div", uj, [n[24] ||= Y("div", null, [
					Y("span", { class: "tv-eyebrow" }, "Fine tuning"),
					Y("h3", null, "Listening controls"),
					Y("p", null, "These controls apply to the selected recognition path and shared voice runtime.")
				], -1), Y("span", null, V(T.value.length) + " " + V(T.value.length === 1 ? "section" : "sections"), 1)]), ga(Vk, {
					sections: T.value,
					values: b.value,
					onChange: I
				}, null, 8, ["sections", "values"])])) : Z("", !0)
			], 64)) : a.value === "replies" ? (q(), da(VA, {
				key: 3,
				scope: "direct",
				draft: e.draft,
				ui: e.speechUi,
				endpoints: e.endpoints,
				onDirty: F,
				onNotify: n[6] ||= (e, t) => i("notify", e, t)
			}, null, 8, [
				"draft",
				"ui",
				"endpoints"
			])) : a.value === "announcements" ? (q(), J(K, { key: 4 }, [ga(VA, {
				scope: "announcement",
				draft: e.draft,
				ui: e.announcementUi,
				endpoints: e.endpoints,
				onDirty: F,
				onNotify: n[7] ||= (e, t) => i("notify", e, t)
			}, null, 8, [
				"draft",
				"ui",
				"endpoints"
			]), Y("article", dj, [Y("header", null, [n[25] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Audio focus"),
				Y("h3", null, "Lower other audio during announcements"),
				Y("p", null, "Fade satellite playback down before Tater speaks, then restore it smoothly.")
			], -1), Y("span", fj, V(Number(e.draft.speech_satellite_ducking_target_percent || 0)) + "% target", 1)]), Y("div", pj, [
				Y("label", mj, [
					n[26] ||= Y("span", { class: "tm-field-label" }, "Background audio target", -1),
					Y("div", hj, [W(Y("input", {
						"onUpdate:modelValue": n[8] ||= (t) => e.draft.speech_satellite_ducking_target_percent = t,
						type: "range",
						min: "0",
						max: "100",
						step: "1",
						onInput: F
					}, null, 544), [[$, e.draft.speech_satellite_ducking_target_percent]]), W(Y("input", {
						"onUpdate:modelValue": n[9] ||= (t) => e.draft.speech_satellite_ducking_target_percent = t,
						type: "number",
						min: "0",
						max: "100",
						onInput: F
					}, null, 544), [[$, e.draft.speech_satellite_ducking_target_percent]])]),
					n[27] ||= Y("small", null, "0% silences other audio; 100% leaves it unchanged.", -1)
				]),
				Y("label", gj, [
					n[28] ||= Y("span", { class: "tm-field-label" }, "Fade down time", -1),
					W(Y("input", {
						"onUpdate:modelValue": n[10] ||= (t) => e.draft.speech_satellite_ducking_attack_ms = t,
						type: "number",
						min: "0",
						step: "10",
						onInput: F
					}, null, 544), [[$, e.draft.speech_satellite_ducking_attack_ms]]),
					n[29] ||= Y("small", null, "Milliseconds before the announcement begins.", -1)
				]),
				Y("label", _j, [
					n[30] ||= Y("span", { class: "tm-field-label" }, "Restore time", -1),
					W(Y("input", {
						"onUpdate:modelValue": n[11] ||= (t) => e.draft.speech_satellite_ducking_release_ms = t,
						type: "number",
						min: "0",
						step: "10",
						onInput: F
					}, null, 544), [[$, e.draft.speech_satellite_ducking_release_ms]]),
					n[31] ||= Y("small", null, "Milliseconds to return to the previous volume.", -1)
				])
			])])], 64)) : (q(), J("article", vj, [
				Y("header", null, [n[32] ||= Y("div", null, [
					Y("span", { class: "tv-eyebrow" }, "Try it now"),
					Y("h3", null, "Voice preview"),
					Y("p", null, "The preview uses the values currently on screen, even before you save them.")
				], -1), Y("span", yj, V(O.value), 1)]),
				Y("div", bj, [Y("button", {
					type: "button",
					class: B({ active: c.value === "direct" }),
					onClick: n[12] ||= (e) => c.value = "direct"
				}, [...n[33] ||= [Y("i", null, "↗", -1), Y("span", null, [Y("strong", null, "Reply voice"), Y("small", null, "Normal conversation")], -1)]], 2), Y("button", {
					type: "button",
					class: B({ active: c.value === "announcement" }),
					onClick: n[13] ||= (e) => c.value = "announcement"
				}, [...n[34] ||= [Y("i", null, "⌂", -1), Y("span", null, [Y("strong", null, "Announcement voice"), Y("small", null, "Proactive and whole-home speech")], -1)]], 2)]),
				Y("div", xj, [Y("label", Sj, [n[35] ||= Y("span", { class: "tm-field-label" }, "What should Tater say?", -1), W(Y("textarea", {
					"onUpdate:modelValue": n[14] ||= (e) => s.value = e,
					placeholder: "Type a short sentence to preview."
				}, null, 512), [[$, s.value]])])]),
				Y("div", Cj, [Y("button", {
					class: "tv-button primary",
					type: "button",
					disabled: l.value || !s.value.trim(),
					onClick: z
				}, V(l.value ? "Generating voice…" : "Generate & play preview"), 9, wj), u.value ? (q(), J("div", Tj, [n[36] ||= Y("span", null, "Latest preview", -1), Y("audio", {
					ref_key: "previewAudio",
					ref: d,
					src: u.value,
					controls: ""
				}, null, 8, Ej)])) : Z("", !0)])
			]))
		]));
	}
}), Oj = { class: "tm-stack tm-wake-workspace" }, kj = {
	key: 0,
	class: "tv-notice"
}, Aj = {
	key: 1,
	class: "tv-notice error"
}, jj = { class: "tm-form-card tm-model-area-hero tm-wake-hero" }, Mj = { class: "tm-model-area-status" }, Nj = {
	key: 2,
	class: "tm-form-card tm-wake-engine-card"
}, Pj = { class: "tm-speech-status-chip" }, Fj = {
	class: "tm-choice-grid tm-wake-choice-grid",
	role: "group",
	"aria-label": "Wake engine"
}, Ij = ["disabled", "onClick"], Lj = {
	key: 0,
	class: "tm-wake-source-panel"
}, Rj = { class: "tm-speech-section-heading compact" }, zj = {
	class: "tm-choice-grid tm-wake-source-grid",
	role: "group",
	"aria-label": "Wake model source"
}, Bj = ["disabled", "onClick"], Vj = {
	key: 0,
	class: "tm-field tm-field-wide"
}, Hj = ["disabled"], Uj = ["value"], Wj = {
	key: 1,
	class: "tm-field tm-field-wide"
}, Gj = ["placeholder", "disabled"], Kj = {
	key: 1,
	class: "tm-wake-engine-note"
}, qj = {
	key: 3,
	class: "tm-form-card tm-wake-training-card"
}, Jj = { class: "tm-wake-feedback-grid" }, Yj = { class: "tm-wake-toggle-card" }, Xj = ["disabled"], Zj = { class: "tm-wake-toggle-card" }, Qj = ["disabled"], $j = { class: "tm-field tm-field-wide" }, eM = ["placeholder", "disabled"], tM = {
	key: 0,
	class: "tm-wake-trainer-linked"
}, nM = ["disabled"], rM = {
	key: 1,
	class: "tm-wake-trainer-link"
}, iM = { class: "tm-inline-actions" }, aM = ["disabled"], oM = ["disabled"], sM = {
	key: 2,
	class: "tm-pairing-box"
}, cM = ["href"], lM = {
	key: 4,
	class: "tm-form-card tm-wake-verifier-card"
}, uM = { class: "tm-speech-status-chip" }, dM = {
	class: "tm-choice-grid tm-wake-verifier-grid",
	role: "group",
	"aria-label": "Wake verification mode"
}, fM = ["disabled", "onClick"], pM = { class: "tm-wake-verifier-summary" }, mM = ["open"], hM = { class: "tm-table-wrap" }, gM = { key: 0 }, _M = ["colspan"], vM = { class: "tm-inline-actions tm-secondary-row" }, yM = ["disabled"], bM = /* @__PURE__ */ sr({
	__name: "WakeWordModels",
	props: {
		runtimeEndpoint: {},
		actionEndpoint: {}
	},
	emits: ["notify", "dirty"],
	setup(e, { expose: t, emit: n }) {
		let r = e, i = n, a = /* @__PURE__ */ U(!1), o = /* @__PURE__ */ U(""), s = /* @__PURE__ */ U(""), c = /* @__PURE__ */ U({}), l = /* @__PURE__ */ U(null), u = /* @__PURE__ */ U(null), d = /* @__PURE__ */ U(null), f = /* @__PURE__ */ Et({}), p = /* @__PURE__ */ Et({}), m = /* @__PURE__ */ U(null), h = /* @__PURE__ */ U(!1), g = null, _ = null, v = Q(() => Array.isArray(c.value.header_stats) ? c.value.header_stats : []), y = Q(() => I(l.value)), b = Q(() => I(u.value)), x = Q(() => String(f.wake_engine || "micro_wake_word")), S = Q(() => String(f.wake_word || "hey_tater")), C = Q(() => Object.values(b.value).find((e) => String(e.type || "") === "select") || {}), w = Q(() => String(C.value.key || "wake_verifier_mode")), T = Q(() => String(p[w.value] || "off")), E = Q(() => Object.values(b.value).find((e) => String(e.type || "") === "table") || {}), D = Q(() => String(Object.values(b.value).find((e) => String(e.key || "").includes("summary"))?.value || "No checks recorded yet")), O = Q(() => String(Object.values(b.value).find((e) => String(e.key || "").includes("stt_engine"))?.value || L("STT Backend") || "Unavailable")), k = Q(() => L("Connected") || "0"), A = Q(() => ne(ee(y.value.wake_engine).find((e) => te(e) === x.value)) || "microWakeWord"), j = Q(() => ne(ee(y.value.wake_word).find((e) => te(e) === S.value)) || "Built-in Hey Tater"), M = {
			micro_wake_word: {
				mark: "MW",
				short: "Private, fast wake detection directly on each satellite."
			},
			button: {
				mark: "BTN",
				short: "Start listening only from the satellite's physical control."
			},
			server: {
				mark: "NET",
				short: "Stream audio to Tater and detect the wake phrase centrally."
			},
			off: {
				mark: "OFF",
				short: "Disable automatic wake detection on every satellite."
			}
		}, N = {
			hey_tater: {
				mark: "T",
				short: "Tater's bundled, ready-to-use wake model."
			},
			catalog: {
				mark: "CAT",
				short: "Choose a versioned model from the official catalog."
			},
			custom_url: {
				mark: "URL",
				short: "Load a compatible microWakeWord JSON package by URL."
			}
		}, P = {
			off: {
				label: "Disabled",
				mark: "OFF",
				short: "Trust the on-device wake model without a second check."
			},
			observe: {
				label: "Observe",
				mark: "OBS",
				short: "Record STT decisions without blocking any wake."
			},
			enforce: {
				label: "Enabled",
				mark: "ON",
				short: "Reject clear transcript mismatches; fail open on errors."
			}
		};
		function F(e) {
			return (e && Array.isArray(e.sections) ? e.sections : []).flatMap((e) => Array.isArray(e.fields) ? e.fields : []);
		}
		function I(e) {
			return Object.fromEntries(F(e).map((e) => [String(e.key || ""), e]).filter(([e]) => e));
		}
		function ee(e) {
			return e && Array.isArray(e.options) ? e.options : [];
		}
		function te(e) {
			return String(e && typeof e == "object" ? e.value ?? e.id ?? "" : e ?? "");
		}
		function ne(e) {
			return String(e && typeof e == "object" ? e.label ?? e.name ?? te(e) : e ?? "");
		}
		function L(e) {
			return String(v.value.find((t) => String(t.label || "").toLowerCase() === e.toLowerCase())?.value ?? "");
		}
		function R(e) {
			se(f, "wake_engine", e);
		}
		function z(e) {
			se(f, "wake_word", e);
		}
		function re(e) {
			se(p, w.value, e);
		}
		function ie(e, t) {
			Object.keys(t).forEach((e) => delete t[e]), (e && Array.isArray(e.sections) ? e.sections : []).forEach((e) => {
				(Array.isArray(e.fields) ? e.fields : []).forEach((e) => {
					let n = String(e.key || "").trim(), r = String(e.type || "").trim().toLowerCase();
					n && ![
						"table",
						"readonly",
						"section",
						"led_preview"
					].includes(r) && !e.disabled && !e.read_only && !e.readonly && (t[n] = e.value ?? e.default ?? "");
				});
			});
		}
		function ae(e, t = !1) {
			let n = e.payload && typeof e.payload == "object" ? e.payload : e;
			c.value = n;
			let r = n.ui && typeof n.ui == "object" ? n.ui : {}, i = Array.isArray(r.item_forms) ? r.item_forms : [];
			l.value = i.find((e) => String(e.group || "") === "global_satellite_model_settings") || null, u.value = i.find((e) => String(e.group || "") === "wake_verifier") || null, d.value = i.find((e) => String(e.group || "") === "wake_trainer_link") || null, t || (ie(l.value, f), ie(u.value, p));
		}
		async function oe(e = !1) {
			e || (a.value = !0), s.value = "";
			try {
				ae(await As(`${r.runtimeEndpoint}?panel=satellites`), e && h.value);
			} catch (e) {
				s.value = e instanceof Error ? e.message : "Wake Word settings could not be loaded.";
			} finally {
				a.value = !1;
			}
		}
		function se(e, t, n) {
			e[t] = n, h.value = !0, s.value = "", i("dirty");
		}
		async function ce(e, t = {}) {
			return js(r.actionEndpoint, {
				action: e,
				payload: t
			});
		}
		async function le() {
			if (o.value) return {
				ok: !1,
				error: "Wake Word settings are busy."
			};
			if (!l.value || !u.value) return {
				ok: !1,
				error: "Wake Word settings are still loading."
			};
			o.value = "apply", s.value = "";
			try {
				let e = String(l.value.save_action || "voice_global_satellite_settings_save"), t = String(u.value.save_action || "voice_wake_verifier_save"), n = await ce(e, {
					id: l.value.id,
					values: { ...f }
				}), r = await ce(t, {
					id: u.value.id,
					values: { ...p }
				});
				h.value = !1, await oe(!0);
				let a = String(r.message || n.message || "Wake Word settings applied to all connected satellites.");
				return i("notify", a, "success"), {
					ok: !0,
					message: a
				};
			} catch (e) {
				return s.value = e instanceof Error ? e.message : "Wake Word settings could not be applied.", i("notify", s.value, "error"), {
					ok: !1,
					error: s.value
				};
			} finally {
				o.value = "";
			}
		}
		async function ue() {
			if (window.confirm(String(u.value?.reset_confirm || "Reset wake-verification statistics?"))) {
				o.value = "reset";
				try {
					let e = await ce(String(u.value?.reset_action || "voice_wake_verifier_stats_reset"));
					i("notify", String(e.message || "Wake-verification statistics reset."), "success"), await oe(!0);
				} catch (e) {
					s.value = e instanceof Error ? e.message : "Statistics could not be reset.";
				} finally {
					o.value = "";
				}
			}
		}
		async function de() {
			if (d.value) {
				o.value = "pair";
				try {
					m.value = await ce(String(d.value.start_action || "voice_wake_trainer_link_pairing_start")), pe();
				} catch (e) {
					s.value = e instanceof Error ? e.message : "Trainer pairing could not start.";
				} finally {
					o.value = "";
				}
			}
		}
		async function fe() {
			let e = String(m.value?.pairing_id || "");
			if (!(!e || !d.value)) try {
				let t = await ce(String(d.value.status_action || "voice_wake_trainer_link_pairing_status"), { values: { pairing_id: e } });
				m.value = t, t.wake_trainer_link && typeof t.wake_trainer_link == "object" && (d.value = t.wake_trainer_link), t.wake_trainer_link?.linked || String(t.status || "") === "linked" ? (me(), i("notify", "Wake Word Trainer linked.", "success"), await oe(!0)) : pe();
			} catch (e) {
				me(), s.value = e instanceof Error ? e.message : "Trainer pairing status could not be checked.";
			}
		}
		function pe() {
			me(), _ = window.setTimeout(fe, 2e3);
		}
		function me() {
			_ !== null && window.clearTimeout(_), _ = null;
		}
		async function he() {
			if (!(!d.value || !window.confirm("Unlink this Wake Word Trainer?"))) {
				o.value = "unlink";
				try {
					let e = await ce(String(d.value.unlink_action || "voice_wake_trainer_link_unlink"));
					d.value = e.wake_trainer_link && typeof e.wake_trainer_link == "object" ? e.wake_trainer_link : d.value, m.value = null, i("notify", "Wake Word Trainer unlinked.", "success");
				} catch (e) {
					s.value = e instanceof Error ? e.message : "Trainer could not be unlinked.";
				} finally {
					o.value = "";
				}
			}
		}
		return Er(() => {
			oe(), g = window.setInterval(() => {
				!o.value && document.visibilityState === "visible" && oe(!0);
			}, 12e3);
		}), kr(() => {
			g !== null && window.clearInterval(g), me();
		}), t({
			apply: le,
			refresh: oe
		}), (e, t) => (q(), J("section", Oj, [
			a.value ? (q(), J("div", kj, "Loading live Wake Word settings…")) : Z("", !0),
			s.value ? (q(), J("div", Aj, V(s.value), 1)) : Z("", !0),
			Y("article", jj, [t[11] ||= Y("div", { class: "tm-model-area-hero-copy" }, [
				Y("span", { class: "tv-eyebrow" }, "Wake pipeline"),
				Y("h3", null, "Say it once. Wake every room."),
				Y("p", null, "Choose how satellites detect a wake, where their model comes from, and whether Tater performs a fast STT verification before opening the microphone.")
			], -1), Y("div", Mj, [
				Y("span", null, [t[10] ||= Y("i", { class: "ready" }, null, -1), X(V(k.value) + " connected", 1)]),
				Y("span", null, V(A.value), 1),
				Y("span", null, V(P[T.value]?.label || "Disabled") + " verification", 1)
			])]),
			l.value ? (q(), J("article", Nj, [
				Y("header", null, [t[12] ||= Y("div", null, [
					Y("span", { class: "tv-eyebrow" }, "Step 1 · Detection"),
					Y("h3", null, "How should Tater wake?"),
					Y("p", null, "Only the settings used by the selected engine are shown.")
				], -1), Y("span", Pj, V(A.value), 1)]),
				Y("div", Fj, [(q(!0), J(K, null, G(ee(y.value.wake_engine), (e) => (q(), J("button", {
					key: te(e),
					type: "button",
					class: B({ active: x.value === te(e) }),
					disabled: !!o.value,
					onClick: (t) => R(te(e))
				}, [
					Y("i", null, V(M[te(e)]?.mark || "WAKE"), 1),
					Y("span", null, [Y("strong", null, V(ne(e)), 1), Y("small", null, V(M[te(e)]?.short), 1)]),
					t[13] ||= Y("b", null, "✓", -1)
				], 10, Ij))), 128))]),
				x.value === "micro_wake_word" ? (q(), J("section", Lj, [
					Y("div", Rj, [t[14] ||= Y("div", null, [Y("h3", null, "Wake model source"), Y("p", null, "Choose Tater's model, a catalog release, or your own package.")], -1), Y("span", null, V(j.value), 1)]),
					Y("div", zj, [(q(!0), J(K, null, G(ee(y.value.wake_word), (e) => (q(), J("button", {
						key: te(e),
						type: "button",
						class: B({ active: S.value === te(e) }),
						disabled: !!o.value,
						onClick: (t) => z(te(e))
					}, [
						Y("i", null, V(N[te(e)]?.mark || "SRC"), 1),
						Y("span", null, [Y("strong", null, V(ne(e)), 1), Y("small", null, V(N[te(e)]?.short), 1)]),
						t[15] ||= Y("b", null, "✓", -1)
					], 10, Bj))), 128))]),
					S.value === "catalog" ? (q(), J("label", Vj, [
						t[16] ||= Y("span", { class: "tm-field-label" }, "Wake Word Catalog", -1),
						W(Y("select", {
							"onUpdate:modelValue": t[0] ||= (e) => f.wake_word_catalog_url = e,
							disabled: !!o.value,
							onChange: t[1] ||= (e) => se(f, "wake_word_catalog_url", f.wake_word_catalog_url)
						}, [(q(!0), J(K, null, G(ee(y.value.wake_word_catalog_url), (e) => (q(), J("option", {
							key: te(e),
							value: te(e)
						}, V(ne(e)), 9, Uj))), 128))], 40, Hj), [[ds, f.wake_word_catalog_url]]),
						Y("small", null, V(y.value.wake_word_catalog_url?.description), 1)
					])) : Z("", !0),
					S.value === "custom_url" ? (q(), J("label", Wj, [
						t[17] ||= Y("span", { class: "tm-field-label" }, "Wake Word JSON URL", -1),
						W(Y("input", {
							"onUpdate:modelValue": t[2] ||= (e) => f.wake_word_url = e,
							type: "url",
							placeholder: String(y.value.wake_word_url?.placeholder || "https://example.local/wake_word.json"),
							disabled: !!o.value,
							onInput: t[3] ||= (e) => se(f, "wake_word_url", f.wake_word_url)
						}, null, 40, Gj), [[$, f.wake_word_url]]),
						Y("small", null, V(y.value.wake_word_url?.description), 1)
					])) : Z("", !0)
				])) : (q(), J("div", Kj, [Y("strong", null, V(A.value) + " selected", 1), Y("span", null, V(M[x.value]?.short) + " Wake-model selection is not needed for this mode.", 1)]))
			])) : Z("", !0),
			l.value ? (q(), J("article", qj, [
				Y("header", null, [t[19] ||= Y("div", null, [
					Y("span", { class: "tv-eyebrow" }, "Step 2 · Improve"),
					Y("h3", null, "Wake Word Trainer"),
					Y("p", null, "Optionally send useful wake clips to your securely linked trainer.")
				], -1), d.value ? (q(), J("span", {
					key: 0,
					class: B(["tv-live-pill", { warning: !d.value.linked }])
				}, [t[18] ||= Y("i", null, null, -1), X(V(d.value.status_label), 1)], 2)) : Z("", !0)]),
				Y("div", Jj, [Y("label", Yj, [t[20] ||= Y("span", null, [Y("strong", null, "Good wakes"), Y("small", null, "Send confirmed wakes to improve recognition.")], -1), W(Y("input", {
					"onUpdate:modelValue": t[4] ||= (e) => f.capture_wake_audio = e,
					type: "checkbox",
					disabled: !!o.value,
					onChange: t[5] ||= (e) => se(f, "capture_wake_audio", f.capture_wake_audio)
				}, null, 40, Xj), [[cs, f.capture_wake_audio]])]), Y("label", Zj, [t[21] ||= Y("span", null, [Y("strong", null, "Close misses"), Y("small", null, "Send near-wakes that can improve model tuning.")], -1), W(Y("input", {
					"onUpdate:modelValue": t[6] ||= (e) => f.capture_close_misses = e,
					type: "checkbox",
					disabled: !!o.value,
					onChange: t[7] ||= (e) => se(f, "capture_close_misses", f.capture_close_misses)
				}, null, 40, Qj), [[cs, f.capture_close_misses]])])]),
				Y("label", $j, [
					t[22] ||= Y("span", { class: "tm-field-label" }, "Trainer App URL", -1),
					W(Y("input", {
						"onUpdate:modelValue": t[8] ||= (e) => f.trainer_app_url = e,
						type: "url",
						placeholder: String(y.value.trainer_app_url?.placeholder || "http://trainer.local:8789"),
						disabled: !!o.value,
						onInput: t[9] ||= (e) => se(f, "trainer_app_url", f.trainer_app_url)
					}, null, 40, eM), [[$, f.trainer_app_url]]),
					t[23] ||= Y("small", null, "The destination used by satellites when wake-clip sharing is enabled.", -1)
				]),
				d.value?.linked ? (q(), J("div", tM, [Y("div", null, [t[24] ||= Y("i", null, "✓", -1), Y("span", null, [Y("strong", null, V(d.value.trainer_name), 1), Y("small", null, "Last model: " + V(d.value.last_wake_word || "No model published yet") + " · " + V(d.value.last_publish_at || "Waiting for first publish"), 1)])]), Y("button", {
					class: "tv-button danger",
					type: "button",
					disabled: !!o.value,
					onClick: he
				}, "Unlink", 8, nM)])) : (q(), J("div", rM, [t[25] ||= Y("div", null, [Y("i", null, "↗"), Y("span", null, [Y("strong", null, "Link the trainer securely"), Y("small", null, "A short pairing code ensures only your trainer can publish wake models.")])], -1), Y("div", iM, [Y("button", {
					class: "tv-button",
					type: "button",
					disabled: !!o.value,
					onClick: de
				}, V(m.value ? "Restart pairing" : "Link trainer"), 9, aM), m.value ? (q(), J("button", {
					key: 0,
					class: "tv-button",
					type: "button",
					disabled: !!o.value,
					onClick: fe
				}, "Check link", 8, oM)) : Z("", !0)])])),
				m.value && !d.value?.linked ? (q(), J("div", sM, [
					t[26] ||= Y("span", null, "Pairing code", -1),
					Y("strong", null, V(m.value.pairing_code || m.value.code || "Waiting…"), 1),
					m.value.pairing_url || m.value.url ? (q(), J("a", {
						key: 0,
						href: String(m.value.pairing_url || m.value.url),
						target: "_blank",
						rel: "noreferrer"
					}, "Open trainer pairing", 8, cM)) : Z("", !0)
				])) : Z("", !0)
			])) : Z("", !0),
			u.value ? (q(), J("article", lM, [
				Y("header", null, [t[27] ||= Y("div", null, [
					Y("span", { class: "tv-eyebrow" }, "Step 3 · Verify"),
					Y("h3", null, "STT wake verification"),
					Y("p", null, "Use the selected Speech STT engine for a fast second opinion after an on-device wake.")
				], -1), Y("span", uM, V(O.value), 1)]),
				Y("div", dM, [(q(!0), J(K, null, G(ee(C.value), (e) => (q(), J("button", {
					key: te(e),
					type: "button",
					class: B({ active: T.value === te(e) }),
					disabled: !!o.value,
					onClick: (t) => re(te(e))
				}, [
					Y("i", null, V(P[te(e)]?.mark), 1),
					Y("span", null, [Y("strong", null, V(P[te(e)]?.label || ne(e)), 1), Y("small", null, V(P[te(e)]?.short), 1)]),
					t[28] ||= Y("b", null, "✓", -1)
				], 10, fM))), 128))]),
				Y("div", pM, [Y("div", null, [t[29] ||= Y("span", null, "Current results", -1), Y("strong", null, V(D.value), 1)]), Y("div", null, [t[30] ||= Y("span", null, "Configured STT", -1), Y("strong", null, V(O.value), 1)])]),
				Array.isArray(E.value.rows) ? (q(), J("details", {
					key: 0,
					class: "tm-wake-results",
					open: !!E.value.rows.length
				}, [Y("summary", null, [Y("span", null, [t[31] ||= Y("strong", null, "Results by satellite", -1), Y("small", null, V(E.value.rows.length) + " satellite" + V(E.value.rows.length === 1 ? "" : "s") + " with verification data", 1)]), t[32] ||= Y("b", null, "⌄", -1)]), Y("div", hM, [Y("table", null, [Y("thead", null, [Y("tr", null, [(q(!0), J(K, null, G(E.value.columns || [], (e) => (q(), J("th", { key: String(e.key) }, V(e.label || e.key), 1))), 128))])]), Y("tbody", null, [(q(!0), J(K, null, G(E.value.rows, (e, t) => (q(), J("tr", { key: t }, [(q(!0), J(K, null, G(E.value.columns || [], (t) => (q(), J("td", { key: String(t.key) }, V(e[String(t.key)] ?? "—"), 1))), 128))]))), 128)), E.value.rows.length ? Z("", !0) : (q(), J("tr", gM, [Y("td", { colspan: Math.max(1, (E.value.columns || []).length) }, "No verifier results yet. Observe mode is a good way to collect data safely.", 8, _M)]))])])])], 8, mM)) : Z("", !0),
				Y("div", vM, [Y("button", {
					class: "tv-button danger",
					type: "button",
					disabled: !!o.value,
					onClick: ue
				}, "Reset Verification Stats", 8, yM)])
			])) : Z("", !0)
		]));
	}
}), xM = { class: "tset-resource tmodels-resource tm-native-models" }, SM = { class: "tv-panel tmodels-hero" }, CM = { class: "tv-live-pill" }, wM = {
	class: "tv-tabs tmodels-tabs",
	"aria-label": "Model settings sections"
}, TM = ["onClick"], EM = { class: "tmodels-context" }, DM = {
	key: 0,
	class: "tv-notice error",
	"aria-live": "polite"
}, OM = {
	key: 1,
	class: "tv-notice",
	"aria-live": "polite"
}, kM = {
	key: 7,
	class: "tm-stack"
}, AM = {
	key: 11,
	class: "tset-save-bar tmodels-save-bar"
}, jM = ["disabled"], MM = /* @__PURE__ */ sr({
	__name: "ModelsSettings",
	props: {
		settings: {},
		endpoints: {},
		initialTab: {},
		onTabChange: { type: Function }
	},
	emits: ["changed", "notify"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = [
			{
				id: "huggingface",
				label: "Hugging Face",
				short: "Find, download, and remove local models."
			},
			{
				id: "routing",
				label: "LLM",
				short: "Base servers, Spudex, Beast roles, and local runtime tuning."
			},
			{
				id: "speech",
				label: "Speech",
				short: "Listening, reply voices, announcements, and playback."
			},
			{
				id: "wake",
				label: "Wake Word",
				short: "Live wake model, trainer, and STT verification settings."
			},
			{
				id: "vision",
				label: "Vision",
				short: "Still-image understanding and multimodal runtime."
			},
			{
				id: "audio-understanding",
				label: "Audio & Video",
				short: "Media understanding for clips and recordings."
			},
			{
				id: "speakerid",
				label: "Speaker ID",
				short: "Live voice identity runtime and enrollment."
			},
			{
				id: "emotionid",
				label: "Emotion ID",
				short: "Live voice-tone classification and prompt hints."
			},
			{
				id: "faceid",
				label: "Face ID",
				short: "Local face recognition model and processing status."
			}
		], a = new Set(i.map((e) => e.id)), o = /* @__PURE__ */ U(O(n.initialTab)), s = /* @__PURE__ */ Et({}), c = /* @__PURE__ */ Et({}), l = /* @__PURE__ */ U(!1), u = /* @__PURE__ */ U(""), d = /* @__PURE__ */ U(""), f = /* @__PURE__ */ U(null), p = /* @__PURE__ */ U(null), m = /* @__PURE__ */ U(null), h = /* @__PURE__ */ U("people"), g = /* @__PURE__ */ U(null), _ = /* @__PURE__ */ U({}), v = /* @__PURE__ */ U(!1), y = /* @__PURE__ */ U({}), b = /* @__PURE__ */ U(""), x = null, S = null, C = Q(() => i.find((e) => e.id === o.value) || i[0]), w = Q(() => s.local_llm_models && typeof s.local_llm_models == "object" ? s.local_llm_models : {}), T = Q(() => {
			switch (o.value) {
				case "routing": return {
					label: "Save & Apply LLM Settings",
					note: "Saves Base, Spudex, and Beast routes, then loads selected local models."
				};
				case "speech": return {
					label: "Save Speech Settings",
					note: "Saves listening, reply, announcement, playback, and native voice-model settings."
				};
				case "wake": return {
					label: "Apply To All Satellites",
					note: "Applies the wake model and verification mode together to every connected satellite."
				};
				case "vision": return {
					label: "Save & Apply Vision Settings",
					note: "Saves image understanding and loads the selected local model when needed."
				};
				case "audio-understanding": return {
					label: "Save & Apply Audio and Video",
					note: "Saves both media configurations and loads selected local models."
				};
				case "speakerid": return h.value === "settings" ? {
					label: "Save Speaker ID Settings",
					note: "Applies the runtime settings above; individual speaker actions remain on their cards."
				} : null;
				case "emotionid": return {
					label: "Save Emotion ID Settings",
					note: "Applies the voice-tone runtime settings above."
				};
				case "faceid": return {
					label: "Apply Face ID Settings",
					note: "Updates the recognition model and live runtime state."
				};
				default: return null;
			}
		}), E = Q(() => s.face_id && typeof s.face_id == "object" ? s.face_id : {}), D = Q(() => Array.isArray(E.value.available_models) ? E.value.available_models : [{
			id: "facenet512",
			label: "FaceNet512"
		}, {
			id: "adaface_ir50_webface4m",
			label: "AdaFace IR-50 · WebFace4M",
			experimental: !0
		}]);
		function O(e) {
			let t = String(e || "").trim().toLowerCase();
			return a.has(t) ? t : "huggingface";
		}
		function k(e) {
			return JSON.parse(JSON.stringify(e ?? {}));
		}
		function A(e) {
			(!e.esphome_settings || typeof e.esphome_settings != "object") && (e.esphome_settings = {});
			let t = e.esphome_settings, n = e.voice_model_ui && typeof e.voice_model_ui == "object" ? e.voice_model_ui : {};
			(Array.isArray(n.sections) ? n.sections : []).forEach((e) => {
				(Array.isArray(e.fields) ? e.fields : []).forEach((e) => {
					let n = String(e.key || "").trim(), r = String(e.type || "").trim().toLowerCase();
					n && ![
						"table",
						"readonly",
						"section",
						"led_preview"
					].includes(r) && !e.disabled && !e.read_only && !e.readonly && !Object.prototype.hasOwnProperty.call(t, n) && (t[n] = e.value ?? e.default ?? (r !== "checkbox" && ""));
				});
			});
		}
		function j(e, t = !1) {
			let n = { ...c };
			Object.keys(s).forEach((e) => delete s[e]), Object.assign(s, k(e || {})), (!Array.isArray(s.hydra_base_servers) || !s.hydra_base_servers.length) && (s.hydra_base_servers = [{
				provider: s.hydra_llm_provider || "openai_compatible",
				host: s.hydra_llm_host || "",
				port: s.hydra_llm_port || "",
				model: s.hydra_llm_model || "",
				api_key: s.hydra_llm_api_key || "",
				llama_cpp_slot: s.hydra_llama_cpp_base_slot || ""
			}]), (!s.face_id || typeof s.face_id != "object") && (s.face_id = {}), A(s), Object.keys(c).forEach((e) => {
				c[e] = t ? !!n[e] : !1;
			});
		}
		function M() {
			c[o.value] = !0, u.value = "", d.value = "";
		}
		function N(e) {
			h.value = e === "settings" ? "settings" : "people";
		}
		function P(e) {
			o.value = O(e), o.value === "speakerid" && (h.value = "people"), u.value = "", d.value = "", n.onTabChange?.(o.value), o.value === "faceid" && me();
		}
		function F(e) {
			return [
				"hf_transformers",
				"llama_cpp",
				"mlx_lm"
			].includes(String(e || ""));
		}
		function I(e, t, n = {}) {
			let r = String(e || ""), i = String(t || "").trim();
			return F(r) && i ? {
				provider: r,
				model: i,
				...n
			} : null;
		}
		function ee(e) {
			let t = /* @__PURE__ */ new Map();
			return e.filter(Boolean).forEach((e) => {
				let n = e, r = `${n.provider}|${n.model}`;
				if (!t.has(r)) t.set(r, n);
				else {
					let e = t.get(r);
					["roles", "media_kinds"].forEach((t) => {
						let r = [...Array.isArray(e[t]) ? e[t] : [], ...Array.isArray(n[t]) ? n[t] : []];
						r.length && (e[t] = Array.from(new Set(r.map(String))));
					});
				}
			}), [...t.values()];
		}
		function te() {
			let e = (Array.isArray(s.hydra_base_servers) ? s.hydra_base_servers : []).map((e) => ({
				provider: String(e.provider || "openai_compatible"),
				host: String(e.host || "").trim(),
				port: String(e.port || "").trim(),
				model: String(e.model || "").trim(),
				api_key: String(e.api_key || "").trim(),
				llama_cpp_slot: String(e.llama_cpp_slot || "").trim()
			})), t = e[0] || {}, n = Object.fromEntries((/* @__PURE__ */ "hydra_hf_transformers_context_tokens.hydra_hf_transformers_device.hydra_hf_transformers_dtype.hydra_hf_transformers_device_map.hydra_hf_transformers_attn_implementation.hydra_hf_transformers_trust_remote_code.hydra_llama_cpp_context_tokens.hydra_llama_cpp_mtp_enabled.hydra_llama_cpp_speculative_method.hydra_llama_cpp_mtp_draft_tokens.hydra_llama_cpp_mtp_draft_model.hydra_llama_cpp_n_batch.hydra_llama_cpp_n_ubatch.hydra_llama_cpp_flash_attn.hydra_llama_cpp_offload_kqv.hydra_llama_cpp_slot_count.hydra_mlx_lm_context_tokens.hydra_mlx_lm_trust_remote_code.hydra_mlx_lm_lazy_load.hydra_mlx_engine_prefill_step_size.hydra_mlx_engine_kv_bits.hydra_mlx_engine_kv_group_size.hydra_mlx_engine_quantized_kv_start.spudex_llm_provider.spudex_llm_host.spudex_llm_model.hydra_beast_mode_enabled".split(".")).map((e) => [e, s[e]]));
			Object.assign(n, {
				hydra_llm_provider: t.provider || "openai_compatible",
				hydra_llm_host: t.host || "",
				hydra_llm_port: t.port || "",
				hydra_llm_model: t.model || "",
				hydra_llm_api_key: t.api_key || "",
				hydra_llama_cpp_base_slot: t.llama_cpp_slot || "",
				hydra_base_servers: e
			});
			let r = e.map((e) => I(e.provider, e.model, { roles: ["Base"] }));
			return s.spudex_llm_provider && r.push(I(s.spudex_llm_provider, s.spudex_llm_model)), [
				"astraeus",
				"thanatos",
				"hermes"
			].forEach((e) => {
				[
					"provider",
					"host",
					"port",
					"model",
					"api_key",
					"llama_cpp_slot"
				].forEach((t) => {
					n[`hydra_llm_${e}_${t}`] = s[`hydra_llm_${e}_${t}`] ?? "";
				}), r.push(I(s[`hydra_llm_${e}_provider`], s[`hydra_llm_${e}_model`], { roles: [e] }));
			}), n.hydra_local_model_load_targets = ee(r), n;
		}
		function ne() {
			let e = (Array.isArray(s.hydra_base_servers) ? s.hydra_base_servers : []).findIndex((e) => F(e.provider) && !String(e.model || "").trim());
			if (e >= 0) return `Choose a downloaded model for ${e === 0 ? "the primary Base server" : `fallback server ${e}`}.`;
			if (s.spudex_llm_provider && F(s.spudex_llm_provider) && !String(s.spudex_llm_model || "").trim()) return "Choose a downloaded model for Spudex.";
			if (s.hydra_beast_mode_enabled) {
				for (let e of [
					"astraeus",
					"thanatos",
					"hermes"
				]) if (F(s[`hydra_llm_${e}_provider`]) && !String(s[`hydra_llm_${e}_model`] || "").trim()) return `Choose a downloaded model for ${e.charAt(0).toUpperCase() + e.slice(1)}.`;
			}
			return "";
		}
		let L = /* @__PURE__ */ "speech_stt_backend.speech_acceleration.speech_wyoming_stt_host.speech_wyoming_stt_port.speech_tts_backend.speech_tts_model.speech_tts_voice.speech_kokoro_output_gain.speech_pocket_tts_output_gain.speech_qwen_tts_clone_text.speech_qwen_tts_language.speech_qwen_tts_instruct.speech_omnivoice_tts_clone_text.speech_omnivoice_tts_language.speech_omnivoice_tts_instruct.speech_wyoming_tts_host.speech_wyoming_tts_port.speech_wyoming_tts_voice.speech_openai_tts_base_url.speech_openai_tts_api_key.speech_chatterbox_tts_base_url.speech_chatterbox_tts_voice_mode.speech_chatterbox_tts_chunk_size.speech_chatterbox_tts_temperature.speech_chatterbox_tts_exaggeration.speech_chatterbox_tts_cfg_weight.speech_chatterbox_tts_seed.speech_chatterbox_tts_speed_factor.speech_chatterbox_tts_language.speech_chatterbox_tts_streaming_enabled.speech_announcement_tts_backend.speech_announcement_tts_model.speech_announcement_tts_voice.speech_announcement_kokoro_output_gain.speech_announcement_pocket_tts_output_gain.speech_announcement_qwen_tts_clone_text.speech_announcement_qwen_tts_language.speech_announcement_qwen_tts_instruct.speech_announcement_omnivoice_tts_clone_text.speech_announcement_omnivoice_tts_language.speech_announcement_omnivoice_tts_instruct.speech_satellite_ducking_target_percent.speech_satellite_ducking_attack_ms.speech_satellite_ducking_release_ms.speech_announcement_wyoming_tts_host.speech_announcement_wyoming_tts_port.speech_announcement_wyoming_tts_voice.speech_announcement_openai_tts_base_url.speech_announcement_openai_tts_api_key.speech_announcement_chatterbox_tts_base_url.speech_announcement_chatterbox_tts_voice_mode.speech_announcement_chatterbox_tts_chunk_size.speech_announcement_chatterbox_tts_temperature.speech_announcement_chatterbox_tts_exaggeration.speech_announcement_chatterbox_tts_cfg_weight.speech_announcement_chatterbox_tts_seed.speech_announcement_chatterbox_tts_speed_factor.speech_announcement_chatterbox_tts_language".split(".");
		function R() {
			return {
				...Object.fromEntries(L.map((e) => [e, s[e]])),
				esphome_settings: k(s.esphome_settings || {})
			};
		}
		function z(e) {
			let t = {}, n = [];
			return e.forEach((e) => {
				let r = e === "vision" ? "vision" : `${e}_understanding`;
				[
					"mode",
					"provider",
					"api_base",
					"model",
					"api_key"
				].forEach((e) => {
					t[`${r}_${e}`] = s[`${r}_${e}`];
				}), e !== "vision" && (t[`${r}_max_seconds`] = Number(s[`${r}_max_seconds`] || (e === "audio" ? 60 : 15))), String(s[`${r}_mode`] || "") === "dedicated" && n.push(I(s[`${r}_provider`], s[`${r}_model`], {
					roles: [e === "vision" ? "Image" : e === "audio" ? "Audio" : "Video"],
					media_kinds: [e]
				}));
			}), e.includes("vision") && (t.hydra_llama_cpp_vision_context_tokens = s.hydra_llama_cpp_vision_context_tokens, t.hydra_llama_cpp_vision_slot = s.hydra_llama_cpp_vision_slot), t.hydra_local_model_load_targets = ee(n), t;
		}
		function re(e) {
			for (let t of e) {
				let e = t === "vision" ? "vision" : `${t}_understanding`;
				if (String(s[`${e}_mode`] || "") === "dedicated" && !String(s[`${e}_model`] || "").trim()) return `Choose a dedicated ${t === "vision" ? "image" : t} model before saving.`;
			}
			return "";
		}
		async function ie(e, t) {
			let i = await js(n.endpoints.models, e);
			r("changed", k(s));
			let a = String(i.message || t);
			return d.value = a, r("notify", a, "success"), i;
		}
		function ae() {
			S !== null && window.clearTimeout(S), S = null;
		}
		function oe() {
			ae(), b.value = "", y.value = {
				running: !0,
				ui_phase: "saving",
				progress: 2,
				items: [],
				errors: [],
				unload_before: [],
				unload_result: {},
				runtime_restart: {},
				load_models: !0
			}, v.value = !0;
		}
		function se(e) {
			let t = e.hf_llm_warmup && typeof e.hf_llm_warmup == "object" ? e.hf_llm_warmup : {};
			y.value = k(t), b.value = "", t.running && ue(450);
		}
		function ce(e) {
			ae(), y.value = {
				...y.value,
				running: !1,
				ui_phase: "",
				finished_ts: Date.now() / 1e3,
				errors: [e]
			}, b.value = "", v.value = !0;
		}
		async function le() {
			S = null;
			try {
				let e = await As(n.endpoints.modelsHfWarmup);
				y.value = e, b.value = "", e.running && ue(700);
			} catch (e) {
				b.value = e instanceof Error ? e.message : "Live model progress is temporarily unavailable.", y.value.running && ue(1600);
			}
		}
		function ue(e = 700) {
			ae(), S = window.setTimeout(le, e);
		}
		async function de() {
			try {
				let e = await As(n.endpoints.modelsHfWarmup);
				e.running && String(e.reason || "").startsWith("settings-save") && (y.value = e, b.value = "", v.value = !0, ue(450));
			} catch {}
		}
		async function fe() {
			if (!T.value || l.value) return;
			l.value = !0, u.value = "", d.value = "";
			let e = !1;
			try {
				let t = {};
				if (o.value === "routing") {
					let n = ne();
					if (n) throw Error(n);
					oe(), e = !0, t = await ie(te(), "LLM settings saved and selected local models queued."), se(t);
				} else if (o.value === "speech") t = await ie(R(), "Speech settings saved."), await g.value?.refreshWarmup?.();
				else if (o.value === "vision") {
					let n = re(["vision"]);
					if (n) throw Error(n);
					oe(), e = !0, t = await ie(z(["vision"]), "Vision settings saved."), se(t);
				} else if (o.value === "audio-understanding") {
					let n = re(["audio", "video"]);
					if (n) throw Error(n);
					oe(), e = !0, t = await ie(z(["audio", "video"]), "Audio and video settings saved."), se(t);
				} else o.value === "faceid" ? (t = await ie({
					face_id_enabled: !!E.value.enabled,
					face_id_model: String(E.value.model_id || "facenet512")
				}, "Face ID settings applied."), await me()) : o.value === "wake" ? t = await f.value?.apply?.() || {
					ok: !1,
					error: "Wake Word settings are not ready."
				} : o.value === "speakerid" ? t = await p.value?.apply?.() || {
					ok: !1,
					error: "Speaker ID settings are not ready."
				} : o.value === "emotionid" && (t = await m.value?.apply?.() || {
					ok: !1,
					error: "Emotion ID settings are not ready."
				});
				if (t && t.ok === !1) throw Error(String(t.error || "Settings could not be applied."));
				c[o.value] = !1;
			} catch (t) {
				u.value = t instanceof Error ? t.message : "Model settings could not be applied.", e && ce(u.value), r("notify", u.value, "error");
			} finally {
				l.value = !1;
			}
		}
		function pe(e) {
			s.local_llm_models = e;
			let t = k(s);
			r("changed", t);
		}
		async function me() {
			try {
				_.value = await As(n.endpoints.modelsFaceStatus);
			} catch (e) {
				u.value = e instanceof Error ? e.message : "Face ID status could not be loaded.";
			}
		}
		return On(() => n.settings, (e) => j(e, !0), { deep: !0 }), Er(() => {
			j(n.settings), n.onTabChange?.(o.value), o.value === "faceid" && me();
			let e = n.settings.hf_llm_warmup && typeof n.settings.hf_llm_warmup == "object" ? n.settings.hf_llm_warmup : {};
			e.running && String(e.reason || "").startsWith("settings-save") && (y.value = k(e), v.value = !0, ue(450)), de(), x = window.setInterval(() => {
				o.value === "faceid" && document.visibilityState === "visible" && me();
			}, 5e3);
		}), kr(() => {
			x !== null && window.clearInterval(x), ae();
		}), (t, n) => (q(), J("section", xM, [
			Y("section", SM, [n[7] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Model workspace"),
				Y("h2", null, "Models, routing, and local runtimes"),
				Y("p", null, "Every Models area is reactive and has one clear save or apply action at its bottom. Downloads, tests, enrollment, and diagnostics remain beside the item they affect.")
			], -1), Y("span", CM, [n[6] ||= Y("i", null, null, -1), X(V(C.value.label), 1)])]),
			Y("nav", wM, [(q(), J(K, null, G(i, (e) => Y("button", {
				key: e.id,
				type: "button",
				class: B({ active: o.value === e.id }),
				onClick: (t) => P(e.id)
			}, V(e.label), 11, TM)), 64))]),
			Y("div", EM, [Y("strong", null, V(C.value.label), 1), Y("span", null, V(C.value.short), 1)]),
			u.value ? (q(), J("div", DM, V(u.value), 1)) : Z("", !0),
			d.value ? (q(), J("div", OM, V(d.value), 1)) : Z("", !0),
			o.value === "huggingface" ? (q(), da(Zw, {
				key: 2,
				"local-models": w.value,
				endpoints: e.endpoints,
				onLocalModels: pe,
				onNotify: n[0] ||= (e, t) => r("notify", e, t)
			}, null, 8, ["local-models", "endpoints"])) : o.value === "routing" ? (q(), da(AO, {
				key: 3,
				draft: s,
				"local-models": w.value,
				"remote-models-endpoint": e.endpoints.modelsRemoteLlm,
				"context-estimate-endpoint": e.endpoints.modelsContextEstimate,
				onDirty: M
			}, null, 8, [
				"draft",
				"local-models",
				"remote-models-endpoint",
				"context-estimate-endpoint"
			])) : o.value === "speech" ? (q(), da(Dj, {
				key: 4,
				ref_key: "speechPanel",
				ref: g,
				draft: s,
				"speech-ui": s.speech_ui || {},
				"announcement-ui": s.announcement_speech_ui || {},
				"voice-model-ui": s.voice_model_ui || {},
				endpoints: e.endpoints,
				onDirty: M,
				onNotify: n[1] ||= (e, t) => r("notify", e, t)
			}, null, 8, [
				"draft",
				"speech-ui",
				"announcement-ui",
				"voice-model-ui",
				"endpoints"
			])) : o.value === "wake" ? (q(), da(bM, {
				key: 5,
				ref_key: "wakePanel",
				ref: f,
				"runtime-endpoint": e.endpoints.modelsVoiceRuntime,
				"action-endpoint": e.endpoints.modelsVoiceAction,
				onDirty: M,
				onNotify: n[2] ||= (e, t) => r("notify", e, t)
			}, null, 8, ["runtime-endpoint", "action-endpoint"])) : o.value === "vision" ? (q(), da(hk, {
				key: 6,
				kind: "vision",
				draft: s,
				"local-models": w.value,
				onDirty: M
			}, null, 8, ["draft", "local-models"])) : o.value === "audio-understanding" ? (q(), J("section", kM, [ga(hk, {
				kind: "audio",
				draft: s,
				"local-models": w.value,
				onDirty: M
			}, null, 8, ["draft", "local-models"]), ga(hk, {
				kind: "video",
				draft: s,
				"local-models": w.value,
				onDirty: M
			}, null, 8, ["draft", "local-models"])])) : o.value === "speakerid" ? (q(), da(wE, {
				key: 8,
				ref_key: "speakerPanel",
				ref: p,
				kind: "speakerid",
				"runtime-endpoint": e.endpoints.modelsVoiceRuntime,
				"action-endpoint": e.endpoints.modelsVoiceAction,
				onDirty: M,
				onSubtab: N,
				onNotify: n[3] ||= (e, t) => r("notify", e, t)
			}, null, 8, ["runtime-endpoint", "action-endpoint"])) : o.value === "emotionid" ? (q(), da(wE, {
				key: 9,
				ref_key: "emotionPanel",
				ref: m,
				kind: "emotionid",
				"runtime-endpoint": e.endpoints.modelsVoiceRuntime,
				"action-endpoint": e.endpoints.modelsVoiceAction,
				onDirty: M,
				onNotify: n[4] ||= (e, t) => r("notify", e, t)
			}, null, 8, ["runtime-endpoint", "action-endpoint"])) : o.value === "faceid" ? (q(), da(fT, {
				key: 10,
				settings: E.value,
				status: _.value,
				models: D.value,
				busy: l.value,
				onDirty: M
			}, null, 8, [
				"settings",
				"status",
				"models",
				"busy"
			])) : Z("", !0),
			T.value ? (q(), J("footer", AM, [Y("div", null, [Y("strong", null, V(c[o.value] ? "Unsaved changes" : `${C.value.label} settings are synchronized`), 1), Y("span", null, V(T.value.note), 1)]), Y("button", {
				class: "tv-button primary",
				type: "button",
				disabled: l.value,
				onClick: fe
			}, V(l.value ? "Applying…" : T.value.label), 9, jM)])) : Z("", !0),
			ga(zO, {
				open: v.value,
				snapshot: y.value,
				"poll-error": b.value,
				onClose: n[5] ||= (e) => v.value = !1
			}, null, 8, [
				"open",
				"snapshot",
				"poll-error"
			])
		]));
	}
}), NM = { class: "tset-resource tpeople" }, PM = { class: "tv-panel tpeople-overview" }, FM = ["disabled"], IM = { class: "tv-metrics tpeople-metrics" }, LM = {
	class: "tv-tabs tpeople-tabs",
	"aria-label": "People sections"
}, RM = { class: "tv-panel tset-form-card" }, zM = { class: "tpeople-section-head" }, BM = { class: "people-create-panel" }, VM = { class: "people-create-body" }, HM = { class: "people-field" }, UM = ["onKeydown"], WM = ["disabled"], GM = {
	key: 0,
	class: "people-directory-grid"
}, KM = ["onClick", "onKeydown"], qM = ["src", "alt"], JM = { key: 1 }, YM = { class: "people-person-card-copy" }, XM = { class: "card-head people-person-card-head" }, ZM = { class: "card-title" }, QM = { class: "people-person-badges" }, $M = {
	key: 0,
	class: "people-badge admin"
}, eN = { class: "people-person-stats" }, tN = { class: "people-person-card-footer" }, nN = ["onClick"], rN = {
	key: 1,
	class: "tv-empty"
}, iN = {
	key: 2,
	class: "tv-panel tset-form-card"
}, aN = {
	key: 0,
	class: "people-identity-list"
}, oN = { class: "people-platform-mark" }, sN = { class: "people-identity-copy" }, cN = { class: "people-identity-title-row" }, lN = { class: "people-identity-actions" }, uN = ["onUpdate:modelValue"], dN = ["value"], fN = ["disabled", "onClick"], pN = ["disabled", "onClick"], mN = {
	key: 1,
	class: "tv-empty"
}, hN = { class: "tv-panel tset-form-card" }, gN = { class: "tpeople-section-head" }, _N = { class: "tpeople-head-actions" }, vN = ["disabled"], yN = {
	key: 0,
	class: "people-face-grid"
}, bN = { class: "people-face-summary" }, xN = { class: "people-face-avatar" }, SN = ["src", "alt"], CN = { key: 1 }, wN = { class: "people-face-copy" }, TN = { class: "people-identity-title-row" }, EN = { class: "people-face-fields" }, DN = { class: "people-field" }, ON = ["onUpdate:modelValue"], kN = ["value"], AN = { class: "people-field" }, jN = ["onUpdate:modelValue"], MN = ["disabled", "onClick"], NN = ["onClick"], PN = { class: "people-face-review-trigger-copy" }, FN = { class: "people-face-footer" }, IN = ["onUpdate:modelValue"], LN = ["value"], RN = ["disabled", "onClick"], zN = ["disabled", "onClick"], BN = {
	key: 1,
	class: "tv-empty"
}, VN = {
	key: 0,
	class: "tv-modal people-person-dialog",
	role: "dialog",
	"aria-modal": "true",
	"aria-labelledby": "people-person-dialog-title"
}, HN = { id: "people-person-dialog-title" }, UN = { class: "people-person-manage-body" }, WN = { class: "people-edit-grid" }, GN = { class: "people-field" }, KN = { class: "tv-toggle people-admin-toggle" }, qN = { class: "people-field people-instructions-field" }, JN = { class: "people-person-actions" }, YN = ["disabled"], XN = ["disabled"], ZN = { class: "people-linked-section" }, QN = {
	key: 0,
	class: "people-alias-list"
}, $N = { class: "people-platform-mark" }, eP = { class: "people-alias-copy" }, tP = ["disabled", "onClick"], nP = {
	key: 1,
	class: "people-empty-inline"
}, rP = {
	key: 0,
	class: "tv-modal people-face-review-dialog",
	role: "dialog",
	"aria-modal": "true",
	"aria-labelledby": "people-face-review-title"
}, iP = { id: "people-face-review-title" }, aP = { class: "people-face-review-toolbar" }, oP = { class: "people-face-review-selection-tools" }, sP = { class: "people-face-selection-count" }, cP = ["disabled"], lP = ["disabled"], uP = { class: "people-face-gallery" }, dP = ["aria-pressed", "onClick"], fP = ["src", "alt"], pP = { class: "people-face-capture-time" }, mP = { class: "people-face-review-actions" }, hP = { class: "people-face-destination" }, gP = ["value"], _P = ["disabled"], vP = ["disabled"], yP = {
	key: 0,
	class: "tv-modal people-face-enroll-dialog",
	role: "dialog",
	"aria-modal": "true",
	"aria-labelledby": "people-face-enroll-title"
}, bP = { class: "people-face-enroll-form" }, xP = { class: "people-field" }, SP = ["value"], CP = { class: "people-field" }, wP = { class: "tpeople-camera-actions" }, TP = ["src"], EP = { class: "people-face-enroll-note" }, DP = ["disabled"], OP = /* @__PURE__ */ sr({
	__name: "PeopleSettings",
	props: {
		payload: {},
		endpoint: {},
		actionEndpoint: {},
		initialTab: { default: "people" },
		initialSort: { default: "recent" }
	},
	emits: [
		"changed",
		"notify",
		"tabChange",
		"sortChange"
	],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ U(n.payload || {}), a = /* @__PURE__ */ U(F(n.initialTab)), o = /* @__PURE__ */ U(I(n.initialSort)), s = /* @__PURE__ */ U(!1), c = /* @__PURE__ */ U(""), l = /* @__PURE__ */ U(""), u = /* @__PURE__ */ U(""), d = /* @__PURE__ */ U(""), f = /* @__PURE__ */ U(""), p = /* @__PURE__ */ U(""), m = /* @__PURE__ */ U([]), h = /* @__PURE__ */ U(""), g = /* @__PURE__ */ U(!1), _ = /* @__PURE__ */ U(""), v = /* @__PURE__ */ U(null), y = /* @__PURE__ */ U(""), b = /* @__PURE__ */ U(!1), x = /* @__PURE__ */ U(null), S = null, C = /* @__PURE__ */ Et({
			display_name: "",
			is_admin: !1,
			instructions: ""
		}), w = /* @__PURE__ */ Et({}), T = /* @__PURE__ */ Et({}), E = Q(() => Array.isArray(i.value.people) ? i.value.people : []), D = Q(() => Array.isArray(i.value.identities) ? i.value.identities : []), O = Q(() => Array.isArray(i.value.faces) ? i.value.faces : []), k = Q(() => Array.isArray(i.value.summary_metrics) ? i.value.summary_metrics : []), A = Q(() => i.value.face_id && typeof i.value.face_id == "object" ? i.value.face_id : {}), j = Q(() => E.value.find((e) => String(e.id || "") === f.value) || null), M = Q(() => O.value.find((e) => String(e.id || "") === p.value) || null), N = Q(() => [...E.value].sort((e, t) => {
			let n = String(e.display_name || "").localeCompare(String(t.display_name || ""), void 0, { sensitivity: "base" }), r = L(e), i = L(t), a = ae(i.last_seen) - ae(r.last_seen), s = Number(!!i.linked) - Number(!!r.linked);
			return o.value === "name" ? n : o.value === "face" ? s || a || n : a || n;
		})), P = Q(() => O.value.map((e) => ({
			face: e,
			draft: ce(e)
		})));
		function F(e) {
			let t = String(e || "").trim();
			return t === "faces" || t === "identities" ? t : "people";
		}
		function I(e) {
			let t = String(e || "").trim();
			return t === "name" || t === "face" ? t : "recent";
		}
		function ee(e) {
			a.value = F(e), r("tabChange", a.value);
		}
		function te() {
			o.value = I(o.value), r("sortChange", o.value);
		}
		function ne(e) {
			return `${String(e.platform || "unknown")}:${String(e.external_id || "")}`;
		}
		function L(e) {
			return e.face_id && typeof e.face_id == "object" ? e.face_id : {};
		}
		function R(e) {
			return e && Array.isArray(e.aliases) ? e.aliases : [];
		}
		function z(e) {
			return e && Array.isArray(e.gallery) ? e.gallery : [];
		}
		function re(e, t = "P") {
			return String(e || "").trim().charAt(0).toUpperCase() || t;
		}
		function ie(e) {
			let t = String(e || "").trim();
			return {
				webui: "WebUI",
				little_spud: "Little Spud",
				homekit: "HomeKit",
				macos: "macOS",
				xbmc: "Kodi"
			}[t] || t.replaceAll("_", " ").replace(/\b\w/g, (e) => e.toUpperCase()) || "Identity";
		}
		function ae(e) {
			if (typeof e == "number") return Number.isFinite(e) ? e * (e < 0xe8d4a51000 ? 1e3 : 1) : 0;
			let t = Number(e);
			if (String(e || "").trim() && Number.isFinite(t)) return t * (t < 0xe8d4a51000 ? 1e3 : 1);
			let n = Date.parse(String(e || ""));
			return Number.isFinite(n) ? n : 0;
		}
		function oe(e) {
			let t = ae(e);
			if (!t) return "Not seen yet";
			let n = new Date(t), r = /* @__PURE__ */ new Date(), i = Math.round((new Date(r.getFullYear(), r.getMonth(), r.getDate()).getTime() - new Date(n.getFullYear(), n.getMonth(), n.getDate()).getTime()) / 864e5), a = n.toLocaleTimeString([], {
				hour: "numeric",
				minute: "2-digit"
			});
			return i === 0 ? `Today at ${a}` : i === 1 ? `Yesterday at ${a}` : n.toLocaleString([], {
				month: "short",
				day: "numeric",
				year: n.getFullYear() === r.getFullYear() ? void 0 : "numeric",
				hour: "numeric",
				minute: "2-digit"
			});
		}
		function se() {
			let e = /* @__PURE__ */ new Set();
			O.value.forEach((t) => {
				let n = String(t.id || "");
				n && (e.add(n), w[n] = {
					person_id: String(t.person_id || ""),
					name: String(t.local_name || ""),
					merge_target: ""
				});
			}), Object.keys(w).forEach((t) => {
				e.has(t) || delete w[t];
			});
			let t = /* @__PURE__ */ new Set();
			D.value.forEach((e) => {
				let n = ne(e);
				t.add(n), T[n] = String(e.person_id || "");
			}), Object.keys(T).forEach((e) => {
				t.has(e) || delete T[e];
			}), f.value && !E.value.some((e) => String(e.id || "") === f.value) && me(), p.value && !O.value.some((e) => String(e.id || "") === p.value) && Ce();
		}
		function ce(e) {
			let t = String(e.id || "");
			return w[t] || (w[t] = {
				person_id: String(e.person_id || ""),
				name: String(e.local_name || ""),
				merge_target: ""
			}), w[t];
		}
		function le(e) {
			i.value = e || {}, se(), r("changed", i.value);
		}
		async function ue() {
			s.value = !0, l.value = "";
			try {
				le(await As(n.endpoint));
			} catch (e) {
				l.value = e instanceof Error ? e.message : "People could not be loaded.";
			} finally {
				s.value = !1;
			}
		}
		async function de(e, t, i) {
			if (c.value) return !1;
			c.value = e, l.value = "", u.value = "";
			try {
				let a = await js(n.actionEndpoint, {
					action: e,
					payload: t
				});
				return a.people && typeof a.people == "object" && le(a.people), u.value = String(a.message || i).trim() || i, r("notify", u.value, "success"), !0;
			} catch (e) {
				return l.value = e instanceof Error ? e.message : "People action failed.", r("notify", l.value, "error"), !1;
			} finally {
				c.value = "";
			}
		}
		async function fe() {
			let e = d.value.trim();
			if (!e) {
				l.value = "Enter a display name.";
				return;
			}
			await de("people_create", { values: { display_name: e } }, "Person created.") && (d.value = "");
		}
		function pe(e) {
			f.value = String(e.id || ""), Object.assign(C, {
				display_name: String(e.display_name || "Person"),
				is_admin: !!e.is_admin,
				instructions: String(e.instructions || "")
			});
		}
		function me() {
			f.value = "";
		}
		async function he() {
			f.value && await de("people_save", {
				person_id: f.value,
				values: { ...C }
			}, "Person saved.") && j.value && pe(j.value);
		}
		async function ge() {
			let e = j.value;
			!e || !window.confirm(`Delete ${String(e.display_name || "this person")}? Identity links will be removed.`) || await de("people_delete", { person_id: String(e.id || "") }, "Person deleted.") && me();
		}
		async function _e(e) {
			f.value && await de("people_alias_detach", {
				person_id: f.value,
				platform: String(e.platform || ""),
				external_id: String(e.external_id || "")
			}, "Identity unlinked.");
		}
		async function ve(e) {
			let t = T[ne(e)] || "";
			if (!t) {
				l.value = "Choose a person for this identity.";
				return;
			}
			await de("people_alias_attach", {
				person_id: t,
				platform: String(e.platform || ""),
				external_id: String(e.external_id || ""),
				label: String(e.label || ""),
				kind: String(e.kind || "user")
			}, "Identity linked.");
		}
		async function ye(e) {
			let t = String(e.label || e.external_id || "this identity");
			window.confirm(`Forget ${ie(e.platform)}: ${t}? Matching discovered chat and memory will be removed.`) && await de("people_identity_forget", {
				platform: String(e.platform || ""),
				external_id: String(e.external_id || "")
			}, "Identity forgotten.");
		}
		async function be(e, t) {
			await de("people_face_save", {
				identity_id: String(e.id || ""),
				values: {
					person_id: t.person_id,
					name: t.name
				}
			}, "Face identity saved.");
		}
		async function xe(e, t) {
			!t.merge_target || !window.confirm("Merge this entire face profile into the selected profile?") || await de("people_face_merge", {
				identity_id: String(e.id || ""),
				values: { target_identity_id: t.merge_target }
			}, "Face profiles merged.");
		}
		async function H(e) {
			window.confirm("Remove this Face ID profile and all of its saved captures?") && await de("people_face_delete", { identity_id: String(e.id || "") }, "Face identity removed.");
		}
		function Se(e) {
			p.value = String(e.id || ""), m.value = [], h.value = "";
		}
		function Ce() {
			p.value = "", m.value = [], h.value = "";
		}
		function we(e) {
			let t = String(e || ""), n = new Set(m.value);
			n.has(t) ? n.delete(t) : n.add(t), m.value = [...n];
		}
		async function Te() {
			if (!M.value || !m.value.length || !h.value) {
				l.value = "Select at least one image and choose a destination.";
				return;
			}
			await de("people_face_move_images", {
				identity_id: String(M.value.id || ""),
				values: {
					target_identity_id: h.value,
					observation_ids: m.value
				}
			}, "Face images moved.") && (m.value = []);
		}
		async function Ee() {
			!M.value || !m.value.length || window.confirm(`Permanently delete ${m.value.length} selected face image${m.value.length === 1 ? "" : "s"}?`) && await de("people_face_remove_images", {
				identity_id: String(M.value.id || ""),
				values: { observation_ids: m.value }
			}, "Face images permanently deleted.") && (m.value = []);
		}
		function De() {
			g.value = !0, _.value = "", v.value = null, y.value = "", l.value = "";
		}
		function Oe() {
			S?.getTracks().forEach((e) => e.stop()), S = null, b.value = !1, x.value && (x.value.srcObject = null);
		}
		function ke() {
			Oe(), g.value = !1;
		}
		async function Ae(e) {
			if (e.size > 8388608) throw Error("The face photo must be 8 MB or smaller.");
			let t = await new Promise((t, n) => {
				let r = new FileReader();
				r.onload = () => t(String(r.result || "")), r.onerror = () => n(/* @__PURE__ */ Error("The selected face photo could not be read.")), r.readAsDataURL(e);
			}), n = t.indexOf(",");
			if (n < 0) throw Error("The selected face photo could not be read.");
			return y.value = t, {
				filename: e.name || "face-photo.jpg",
				content_type: e.type || "image/jpeg",
				data_b64: t.slice(n + 1)
			};
		}
		async function je(e) {
			let t = e.target.files?.[0];
			if (t) try {
				v.value = await Ae(t);
			} catch (e) {
				l.value = e instanceof Error ? e.message : "The face photo could not be read.";
			}
		}
		async function Me() {
			if (!navigator.mediaDevices?.getUserMedia) {
				l.value = "Camera capture requires HTTPS or localhost and a supported browser.";
				return;
			}
			try {
				Oe(), S = await navigator.mediaDevices.getUserMedia({
					video: { facingMode: "user" },
					audio: !1
				}), b.value = !0, await un(), x.value && (x.value.srcObject = S, await x.value.play());
			} catch (e) {
				l.value = e instanceof Error ? e.message : "Camera access failed.", Oe();
			}
		}
		function Ne() {
			let e = x.value;
			if (!e || !e.videoWidth || !e.videoHeight) {
				l.value = "Wait for the camera preview before taking the photo.";
				return;
			}
			let t = document.createElement("canvas");
			t.width = e.videoWidth, t.height = e.videoHeight, t.getContext("2d")?.drawImage(e, 0, 0);
			let n = t.toDataURL("image/jpeg", .9);
			y.value = n, v.value = {
				filename: "camera-face.jpg",
				content_type: "image/jpeg",
				data_b64: n.split(",", 2)[1] || ""
			}, Oe();
		}
		async function Pe() {
			if (!_.value) {
				l.value = "Choose a person for this face.";
				return;
			}
			if (!v.value) {
				l.value = "Choose a face photo or take one with the camera first.";
				return;
			}
			await de("people_face_enroll", { values: {
				person_id: _.value,
				face_image: v.value
			} }, "Face added to person.") && ke();
		}
		return On(() => n.payload, (e) => {
			i.value = e || {}, se();
		}, { immediate: !0 }), kr(Oe), (e, t) => (q(), J("section", NM, [
			u.value || l.value ? (q(), J("div", {
				key: 0,
				class: B(["tv-notice", { error: !!l.value }]),
				"aria-live": "polite"
			}, V(l.value || u.value), 3)) : Z("", !0),
			Y("section", PM, [Y("header", null, [t[12] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Identity directory"),
				Y("h2", null, "Everyone Tater knows"),
				Y("p", null, "Bring faces, portal users, and voice speakers together as the same person.")
			], -1), Y("button", {
				class: "tv-button",
				type: "button",
				disabled: s.value,
				onClick: ue
			}, V(s.value ? "Refreshing…" : "Refresh"), 9, FM)]), Y("div", IM, [(q(!0), J(K, null, G(k.value, (e) => (q(), J("div", { key: e.label }, [Y("span", null, V(e.label), 1), Y("strong", null, V(Number(e.value || 0)), 1)]))), 128))])]),
			Y("nav", LM, [
				Y("button", {
					type: "button",
					class: B({ active: a.value === "people" }),
					onClick: t[0] ||= (e) => ee("people")
				}, "People", 2),
				Y("button", {
					type: "button",
					class: B({ active: a.value === "faces" }),
					onClick: t[1] ||= (e) => ee("faces")
				}, "Faces", 2),
				Y("button", {
					type: "button",
					class: B({ active: a.value === "identities" }),
					onClick: t[2] ||= (e) => ee("identities")
				}, "Identities", 2)
			]),
			a.value === "people" ? (q(), J(K, { key: 1 }, [Y("section", RM, [Y("header", zM, [t[15] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Directory"),
				Y("h2", null, "People"),
				Y("p", null, "Open a card to manage access, instructions, and linked identities.")
			], -1), Y("label", null, [t[14] ||= X("Sort", -1), W(Y("select", {
				"onUpdate:modelValue": t[3] ||= (e) => o.value = e,
				onChange: te
			}, [...t[13] ||= [
				Y("option", { value: "recent" }, "Recently seen", -1),
				Y("option", { value: "name" }, "Name", -1),
				Y("option", { value: "face" }, "Face ID linked", -1)
			]], 544), [[ds, o.value]])])]), Y("details", BM, [t[17] ||= Y("summary", null, "Add a person", -1), Y("div", VM, [Y("label", HM, [t[16] ||= X("Display name", -1), W(Y("input", {
				"onUpdate:modelValue": t[4] ||= (e) => d.value = e,
				type: "text",
				placeholder: "Fred",
				onKeydown: Ss(bs(fe, ["prevent"]), ["enter"])
			}, null, 40, UM), [[$, d.value]])]), Y("button", {
				class: "tv-button primary",
				type: "button",
				disabled: !!c.value,
				onClick: fe
			}, "Create person", 8, WM)])])]), N.value.length ? (q(), J("div", GM, [(q(!0), J(K, null, G(N.value, (e) => (q(), J("article", {
				key: e.id,
				class: "card people-person-card"
			}, [Y("div", {
				class: "people-person-card-main tpeople-card-button",
				role: "button",
				tabindex: "0",
				onClick: (t) => pe(e),
				onKeydown: [Ss(bs((t) => pe(e), ["prevent"]), ["enter"]), Ss(bs((t) => pe(e), ["prevent"]), ["space"])]
			}, [Y("div", { class: B(["people-person-avatar", { "has-image": !!L(e).image_src }]) }, [L(e).image_src ? (q(), J("img", {
				key: 0,
				src: L(e).image_src,
				alt: `${e.display_name} Face ID profile`,
				loading: "lazy"
			}, null, 8, qM)) : (q(), J("span", JM, V(re(e.display_name)), 1))], 2), Y("div", YM, [
				Y("div", XM, [Y("div", null, [Y("h3", ZM, V(e.display_name || "Person"), 1), Y("div", QM, [Y("span", { class: B(["people-badge", L(e).linked ? "face" : "muted"]) }, V(L(e).linked ? "Face ID" : "No Face ID"), 3), e.is_admin ? (q(), J("span", $M, "Admin")) : Z("", !0)])])]),
				Y("div", { class: B(["people-last-seen", { "is-seen": !!L(e).last_seen }]) }, [t[18] ||= Y("span", { class: "people-presence-dot" }, null, -1), Y("span", null, V(L(e).last_seen_camera ? `Last seen at ${L(e).last_seen_camera} · ` : "") + V(oe(L(e).last_seen)), 1)], 2),
				Y("div", eN, [
					Y("span", null, [Y("strong", null, V(R(e).length), 1), t[19] ||= X(" linked identities", -1)]),
					Y("span", null, [Y("strong", null, V(Number(L(e).identity_count || 0)), 1), t[20] ||= X(" face profiles", -1)]),
					Y("span", null, [Y("strong", null, V(Number(L(e).capture_count || 0)), 1), t[21] ||= X(" captures", -1)])
				])
			])], 40, KM), Y("div", tN, [Y("button", {
				class: "people-person-open",
				type: "button",
				onClick: (t) => pe(e)
			}, "Manage person", 8, nN)])]))), 128))])) : (q(), J("p", rN, "No people yet. Create a person, then link discovered identities."))], 64)) : a.value === "identities" ? (q(), J("section", iN, [t[23] ||= Y("header", null, [
				Y("span", { class: "tv-eyebrow" }, "Discovered accounts"),
				Y("h2", null, "Identities"),
				Y("p", null, "Link portal, WebUI, voice, and memory identities to the correct person.")
			], -1), D.value.length ? (q(), J("div", aN, [(q(!0), J(K, null, G(D.value, (e) => (q(), J("article", {
				key: ne(e),
				class: "people-identity-card"
			}, [
				Y("span", oN, V(re(ie(e.platform), "I")), 1),
				Y("div", sN, [
					Y("div", cN, [Y("strong", null, V(e.label || e.external_id), 1), Y("span", { class: B(["people-badge", e.person_id ? "linked" : "muted"]) }, V(e.person_id ? `Linked to ${e.person_name}` : "Unlinked"), 3)]),
					Y("span", null, V([
						ie(e.platform),
						String(e.kind || "user").replaceAll("_", " "),
						e.source,
						Number(e.fact_count || 0) ? `${Number(e.fact_count)} facts` : ""
					].filter(Boolean).join(" · ")), 1),
					Y("small", null, V(e.external_id), 1)
				]),
				Y("div", lN, [
					W(Y("select", { "onUpdate:modelValue": (t) => T[ne(e)] = t }, [t[22] ||= Y("option", { value: "" }, "Choose person…", -1), (q(!0), J(K, null, G(E.value, (e) => (q(), J("option", {
						key: e.id,
						value: e.id
					}, V(e.display_name), 9, dN))), 128))], 8, uN), [[ds, T[ne(e)]]]),
					Y("button", {
						class: "tv-button",
						type: "button",
						disabled: !!c.value || !T[ne(e)],
						onClick: (t) => ve(e)
					}, V(e.person_id ? "Update link" : "Link"), 9, fN),
					e.forgettable && !e.person_id ? (q(), J("button", {
						key: 0,
						class: "tv-button danger",
						type: "button",
						disabled: !!c.value,
						onClick: (t) => ye(e)
					}, "Forget", 8, pN)) : Z("", !0)
				])
			]))), 128))])) : (q(), J("p", mN, "No portal or voice identities have been discovered yet."))])) : (q(), J(K, { key: 3 }, [Y("section", hN, [Y("header", gN, [t[24] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Face ID"),
				Y("h2", null, "Known faces"),
				Y("p", null, "Link faces to people, review captures, and merge duplicates.")
			], -1), Y("div", _N, [Y("span", { class: B(["people-badge", A.value.loaded ? "linked" : "muted"]) }, V(A.value.loaded ? "Model ready" : "Model not ready"), 3), Y("button", {
				class: "tv-button primary",
				type: "button",
				disabled: !E.value.length,
				onClick: De
			}, "Add a face", 8, vN)])])]), P.value.length ? (q(), J("div", yN, [(q(!0), J(K, null, G(P.value, (e) => (q(), J("article", {
				key: e.face.id,
				class: "people-face-card"
			}, [
				Y("div", bN, [Y("div", xN, [e.face.image_src ? (q(), J("img", {
					key: 0,
					src: e.face.image_src,
					alt: e.face.name || "Unknown face",
					loading: "lazy"
				}, null, 8, SN)) : (q(), J("span", CN, V(re(e.face.name, "?")), 1))]), Y("div", wN, [
					Y("div", TN, [Y("strong", null, V(e.face.name || `Unknown face · ${String(e.face.id).slice(-6)}`), 1), Y("span", { class: B(["people-badge", e.face.person_id ? "linked" : "muted"]) }, V(e.face.person_id ? `Linked to ${e.face.person_name}` : "Not linked"), 3)]),
					Y("span", null, V(Number(e.face.capture_count || 0)) + " captures · " + V(Number(e.face.event_count || 0)) + " events", 1),
					Y("small", null, V(oe(e.face.last_seen)), 1)
				])]),
				Y("div", EN, [
					Y("label", DN, [t[26] ||= X("Person", -1), W(Y("select", { "onUpdate:modelValue": (t) => e.draft.person_id = t }, [t[25] ||= Y("option", { value: "" }, "Not linked", -1), (q(!0), J(K, null, G(E.value, (e) => (q(), J("option", {
						key: e.id,
						value: e.id
					}, V(e.display_name), 9, kN))), 128))], 8, ON), [[ds, e.draft.person_id]])]),
					Y("label", AN, [t[27] ||= X("Face name", -1), W(Y("input", {
						"onUpdate:modelValue": (t) => e.draft.name = t,
						type: "text",
						maxlength: "80",
						placeholder: "Used when not linked"
					}, null, 8, jN), [[$, e.draft.name]])]),
					Y("button", {
						class: "tv-button",
						type: "button",
						disabled: !!c.value,
						onClick: (t) => be(e.face, e.draft)
					}, "Save", 8, MN)
				]),
				z(e.face).length ? (q(), J("button", {
					key: 0,
					class: "people-face-review-trigger",
					type: "button",
					onClick: (t) => Se(e.face)
				}, [Y("span", PN, [t[28] ||= Y("small", null, "Saved face images", -1), Y("strong", null, "Review " + V(z(e.face).length) + " images", 1)]), t[29] ||= Y("span", { class: "people-face-review-trigger-action" }, "Open gallery →", -1)], 8, NN)) : Z("", !0),
				Y("div", FN, [
					W(Y("select", { "onUpdate:modelValue": (t) => e.draft.merge_target = t }, [t[30] ||= Y("option", { value: "" }, "Merge profile into…", -1), (q(!0), J(K, null, G(O.value.filter((t) => t.id !== e.face.id), (e) => (q(), J("option", {
						key: e.id,
						value: e.id
					}, V(e.name || `Unknown face · ${String(e.id).slice(-6)}`), 9, LN))), 128))], 8, IN), [[ds, e.draft.merge_target]]),
					Y("button", {
						class: "tv-button",
						type: "button",
						disabled: !!c.value || !e.draft.merge_target,
						onClick: (t) => xe(e.face, e.draft)
					}, "Merge", 8, RN),
					Y("button", {
						class: "tv-button danger",
						type: "button",
						disabled: !!c.value,
						onClick: (t) => H(e.face)
					}, "Remove profile", 8, zN)
				])
			]))), 128))])) : (q(), J("p", BN, V(A.value.enabled ? "No faces have been added yet." : "Face ID is disabled. Enable it under Models › Face ID."), 1))], 64)),
			ga(kl, {
				open: !!j.value,
				"backdrop-class": "tv-modal-backdrop tpeople tset-modal",
				onClose: me
			}, {
				default: Sn(() => [j.value ? (q(), J("section", VN, [Y("header", null, [Y("div", null, [t[31] ||= Y("span", { class: "tv-eyebrow" }, "Tater person", -1), Y("h2", HN, V(j.value.display_name), 1)]), Y("button", {
					class: "tv-button",
					type: "button",
					onClick: me
				}, "Close")]), Y("div", UN, [
					Y("div", WN, [Y("label", GN, [t[32] ||= X("Display name", -1), W(Y("input", {
						"onUpdate:modelValue": t[5] ||= (e) => C.display_name = e,
						type: "text"
					}, null, 512), [[$, C.display_name]])]), Y("label", KN, [W(Y("input", {
						"onUpdate:modelValue": t[6] ||= (e) => C.is_admin = e,
						class: "tv-checkbox",
						type: "checkbox"
					}, null, 512), [[cs, C.is_admin]]), t[33] ||= Y("span", null, [Y("strong", null, "Admin access"), Y("small", null, "Allow admin-only tools from linked identities.")], -1)])]),
					Y("label", qN, [
						t[34] ||= X("Response instructions", -1),
						W(Y("textarea", {
							"onUpdate:modelValue": t[7] ||= (e) => C.instructions = e,
							rows: "4",
							placeholder: "Always call this person sir."
						}, null, 512), [[$, C.instructions]]),
						t[35] ||= Y("small", null, "Used only when Tater resolves the current user to this person.", -1)
					]),
					Y("div", JN, [Y("button", {
						class: "tv-button primary",
						type: "button",
						disabled: !!c.value,
						onClick: he
					}, "Save person", 8, YN), Y("button", {
						class: "tv-button danger",
						type: "button",
						disabled: !!c.value,
						onClick: ge
					}, "Delete", 8, XN)]),
					Y("section", ZN, [t[36] ||= Y("div", { class: "people-section-label" }, "Linked identities", -1), R(j.value).length ? (q(), J("div", QN, [(q(!0), J(K, null, G(R(j.value), (e) => (q(), J("div", {
						key: ne(e),
						class: "people-alias-row"
					}, [
						Y("span", $N, V(re(ie(e.platform), "I")), 1),
						Y("div", eP, [
							Y("strong", null, V(e.label || e.external_id), 1),
							Y("span", null, V(ie(e.platform)) + " · " + V(String(e.kind || "user").replaceAll("_", " ")), 1),
							Y("small", null, V(e.external_id), 1)
						]),
						Y("button", {
							class: "tv-button danger",
							type: "button",
							disabled: !!c.value,
							onClick: (t) => _e(e)
						}, "Unlink", 8, tP)
					]))), 128))])) : (q(), J("p", nP, "No identities linked yet."))])
				])])) : Z("", !0)]),
				_: 1
			}, 8, ["open"]),
			ga(kl, {
				open: !!M.value,
				"backdrop-class": "tv-modal-backdrop tpeople tset-modal",
				onClose: Ce
			}, {
				default: Sn(() => [M.value ? (q(), J("section", rP, [
					Y("header", null, [Y("div", null, [
						t[37] ||= Y("span", { class: "tv-eyebrow" }, "Face ID gallery", -1),
						Y("h2", iP, V(M.value.name || "Unknown face"), 1),
						Y("p", null, V(z(M.value).length) + " saved images · select the captures you want to organize.", 1)
					]), Y("button", {
						class: "tv-button",
						type: "button",
						onClick: Ce
					}, "Close")]),
					Y("div", aP, [t[38] ||= Y("div", null, [Y("strong", null, "Choose saved images"), Y("span", null, "Selected images can be moved or permanently deleted.")], -1), Y("div", oP, [
						Y("span", sP, V(m.value.length) + " selected", 1),
						Y("button", {
							class: "tv-button",
							type: "button",
							disabled: m.value.length === z(M.value).length,
							onClick: t[8] ||= (e) => m.value = z(M.value).map((e) => String(e.id))
						}, "Select all", 8, cP),
						Y("button", {
							class: "tv-button",
							type: "button",
							disabled: !m.value.length,
							onClick: t[9] ||= (e) => m.value = []
						}, "Clear", 8, lP)
					])]),
					Y("div", uP, [(q(!0), J(K, null, G(z(M.value), (e) => (q(), J("button", {
						key: e.id,
						class: B(["people-face-capture", { "is-selected": m.value.includes(String(e.id)) }]),
						type: "button",
						"aria-pressed": m.value.includes(String(e.id)),
						onClick: (t) => we(e.id)
					}, [
						Y("img", {
							src: e.image_src,
							alt: `Face captured ${oe(e.seen_at)}`,
							loading: "lazy"
						}, null, 8, fP),
						Y("span", pP, V(oe(e.seen_at)), 1),
						t[39] ||= Y("span", { class: "people-face-selection-mark" }, "✓", -1)
					], 10, dP))), 128))]),
					Y("div", mP, [
						Y("label", hP, [t[42] ||= X("Move selected images to", -1), W(Y("select", { "onUpdate:modelValue": t[10] ||= (e) => h.value = e }, [
							t[40] ||= Y("option", { value: "" }, "Choose face profile…", -1),
							(q(!0), J(K, null, G(O.value.filter((e) => e.id !== M.value?.id), (e) => (q(), J("option", {
								key: e.id,
								value: e.id
							}, V(e.name || `Unknown face · ${String(e.id).slice(-6)}`), 9, gP))), 128)),
							t[41] ||= Y("option", { value: "__new_unknown__" }, "New unknown face", -1)
						], 512), [[ds, h.value]])]),
						Y("button", {
							class: "tv-button primary",
							type: "button",
							disabled: !!c.value || !m.value.length || !h.value,
							onClick: Te
						}, "Move selected", 8, _P),
						Y("button", {
							class: "tv-button danger",
							type: "button",
							disabled: !!c.value || !m.value.length,
							onClick: Ee
						}, "Permanently delete", 8, vP)
					])
				])) : Z("", !0)]),
				_: 1
			}, 8, ["open"]),
			ga(kl, {
				open: g.value,
				"backdrop-class": "tv-modal-backdrop tpeople tset-modal",
				onClose: ke
			}, {
				default: Sn(() => [g.value ? (q(), J("section", yP, [Y("header", null, [t[43] ||= Y("div", null, [
					Y("span", { class: "tv-eyebrow" }, "Face ID enrollment"),
					Y("h2", { id: "people-face-enroll-title" }, "Add a face"),
					Y("p", null, "Choose a person and provide one clear, front-facing photo.")
				], -1), Y("button", {
					class: "tv-button",
					type: "button",
					onClick: ke
				}, "Close")]), Y("div", bP, [
					Y("label", xP, [t[45] ||= X("Person", -1), W(Y("select", { "onUpdate:modelValue": t[11] ||= (e) => _.value = e }, [t[44] ||= Y("option", { value: "" }, "Choose a person", -1), (q(!0), J(K, null, G(E.value, (e) => (q(), J("option", {
						key: e.id,
						value: e.id
					}, V(e.display_name), 9, SP))), 128))], 512), [[ds, _.value]])]),
					Y("label", CP, [t[46] ||= X("Face photo", -1), Y("input", {
						type: "file",
						accept: "image/jpeg,image/png,image/webp",
						capture: "user",
						onChange: je
					}, null, 32)]),
					Y("div", wP, [
						Y("button", {
							class: "tv-button",
							type: "button",
							onClick: Me
						}, "Use camera"),
						b.value ? (q(), J("button", {
							key: 0,
							class: "tv-button primary",
							type: "button",
							onClick: Ne
						}, "Take photo")) : Z("", !0),
						b.value ? (q(), J("button", {
							key: 1,
							class: "tv-button",
							type: "button",
							onClick: Oe
						}, "Stop camera")) : Z("", !0)
					]),
					W(Y("video", {
						ref_key: "cameraVideo",
						ref: x,
						class: "tpeople-camera",
						muted: "",
						playsinline: ""
					}, null, 512), [[Oo, b.value]]),
					y.value ? (q(), J("img", {
						key: 0,
						class: "tpeople-enrollment-preview",
						src: y.value,
						alt: "Face enrollment preview"
					}, null, 8, TP)) : Z("", !0),
					Y("div", EP, V(A.value.loaded ? "Face ID is ready. The photo will be checked before it is saved." : "The Face ID model is not ready. Enable or load it under Models before adding a face."), 1),
					Y("button", {
						class: "tv-button primary",
						type: "button",
						disabled: !!c.value || !_.value || !v.value,
						onClick: Pe
					}, V(c.value === "people_face_enroll" ? "Adding…" : "Add face to person"), 9, DP)
				])])) : Z("", !0)]),
				_: 1
			}, 8, ["open"])
		]));
	}
}), kP = { class: "tset-resource tredis-resource" }, AP = { class: "tv-panel tredis-hero" }, jP = { class: "tredis-hero-actions" }, MP = ["disabled"], NP = { class: "tm-metrics tredis-metrics" }, PP = {
	key: 1,
	class: "tv-notice error",
	"aria-live": "polite"
}, FP = { class: "tredis-grid" }, IP = { class: "tv-panel tset-form-card tredis-connection-card" }, LP = { class: "tv-panel-head" }, RP = { class: "tv-form-grid" }, zP = ["disabled"], BP = { key: 0 }, VP = ["disabled"], HP = ["disabled"], UP = ["disabled"], WP = ["disabled"], GP = ["placeholder", "disabled"], KP = ["disabled"], qP = { class: "tv-toggle tredis-toggle" }, JP = ["disabled"], YP = { class: "tv-toggle tredis-toggle" }, XP = ["disabled"], ZP = {
	key: 0,
	class: "full"
}, QP = ["disabled"], $P = { class: "tredis-card-actions" }, eF = ["disabled"], tF = ["disabled", "title"], nF = { class: "tv-panel tset-form-card tredis-encryption-card" }, rF = { class: "tv-panel-head" }, iF = { class: "tredis-details" }, aF = { class: "tredis-card-actions" }, oF = ["disabled"], sF = ["disabled"], cF = { class: "tv-panel tredis-runtime-card" }, lF = { class: "tredis-details compact" }, uF = { class: "tset-save-bar tredis-save-bar" }, dF = ["disabled"], fF = /* @__PURE__ */ sr({
	__name: "RedisSettings",
	props: {
		initialStatus: {},
		initialEncryptionStatus: {},
		statusEndpoint: {},
		configureEndpoint: {},
		migrateEndpoint: {},
		encryptionStatusEndpoint: {},
		encryptEndpoint: {},
		decryptEndpoint: {}
	},
	emits: ["status", "notify"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ Et({
			mode: "internal",
			data_path: "",
			host: "",
			port: 6379,
			db: 0,
			username: "",
			password: "",
			use_tls: !1,
			verify_tls: !0,
			ca_cert_path: ""
		}), a = /* @__PURE__ */ U({}), o = /* @__PURE__ */ U({}), s = /* @__PURE__ */ U(!1), c = /* @__PURE__ */ U(""), l = /* @__PURE__ */ U(!1), u = /* @__PURE__ */ U(""), d = /* @__PURE__ */ U(""), f = null;
		function p(e) {
			return String(e ?? "").trim();
		}
		function m(e) {
			let t = e && typeof e == "object" ? e : {}, n = p(t.mode).toLowerCase() === "external" && !t.internal ? "external" : "internal";
			return {
				...t,
				mode: n,
				internal: n === "internal",
				configured: !!t.configured,
				connected: !!t.connected,
				host: p(t.host),
				port: Number(t.port || 6379),
				db: Number(t.db || 0),
				username: p(t.username),
				use_tls: !!t.use_tls,
				verify_tls: t.verify_tls !== !1,
				ca_cert_path: p(t.ca_cert_path),
				password_set: !!t.password_set,
				error: p(t.error),
				fallback_reason: p(t.fallback_reason),
				source: p(t.source),
				config_path: p(t.config_path),
				data_path: p(t.data_path),
				data_dir: p(t.data_dir),
				socket_path: p(t.socket_path),
				redis_pid: Number(t.redis_pid || 0),
				redis_managed: !!t.redis_managed,
				redis_server_source: p(t.redis_server_source)
			};
		}
		function h(e) {
			let t = e && typeof e == "object" ? e : {}, n = t.live_encryption_enabled;
			return {
				...t,
				encryption_available: t.encryption_available !== !1,
				key_exists: !!t.key_exists,
				key_path: p(t.key_path),
				key_fingerprint: p(t.key_fingerprint),
				live_encryption_enabled: n === void 0 ? !!t.snapshot_exists : !!n,
				live_encryption_state_path: p(t.live_encryption_state_path || t.snapshot_path),
				live_encryption_updated: p(t.live_encryption_updated || t.snapshot_modified),
				error: p(t.error)
			};
		}
		function g(e) {
			Object.assign(i, {
				mode: e.internal || e.mode !== "external" ? "internal" : "external",
				data_path: p(e.data_path),
				host: p(e.host),
				port: Number(e.port || 6379),
				db: Number(e.db || 0),
				username: p(e.username),
				password: "",
				use_tls: !!e.use_tls,
				verify_tls: e.verify_tls !== !1,
				ca_cert_path: p(e.ca_cert_path)
			});
		}
		function _(e, t = !s.value) {
			let n = m(e);
			a.value = n, t && g(n), r("status", n);
		}
		function v() {
			s.value = !0, d.value = "", u.value = "";
		}
		function y(e) {
			return {
				mode: i.mode,
				host: p(i.host),
				port: Number(i.port || 6379),
				db: Number(i.db || 0),
				username: p(i.username),
				password: i.password,
				use_tls: !!i.use_tls,
				verify_tls: !!i.verify_tls,
				ca_cert_path: p(i.ca_cert_path),
				data_path: p(i.data_path),
				keep_existing_password: !i.password && !!a.value.password_set,
				test_only: e
			};
		}
		function b() {
			return i.mode === "external" && !p(i.host) ? "Enter the external Redis host." : !Number.isInteger(Number(i.port)) || Number(i.port) < 1 || Number(i.port) > 65535 ? "Redis port must be between 1 and 65535." : !Number.isInteger(Number(i.db)) || Number(i.db) < 0 ? "Redis DB must be zero or greater." : "";
		}
		function x(e, t = "success") {
			t === "error" ? (u.value = e, d.value = "") : (d.value = e, u.value = ""), r("notify", e, t);
		}
		function S(e) {
			if (!e || typeof e != "object") return "";
			let t = e;
			return t.ok === !1 ? ` Startup replay failed: ${p(t.error) || "unknown error"}.` : ` Startup replay complete (restore ${t.ran_restore ? "ran" : "skipped"}; autostart ${t.ran_autostart ? "ran" : "skipped"}).`;
		}
		async function C(e = !1, t = !1) {
			if (c.value && !t) return;
			l.value = !0;
			let i = "";
			try {
				_(await As(n.statusEndpoint), !s.value);
			} catch (e) {
				i = e instanceof Error ? e.message : "Redis connection status could not be loaded.";
			}
			try {
				o.value = h(await As(n.encryptionStatusEndpoint));
			} catch (e) {
				let t = e instanceof Error ? e.message : "Redis encryption status could not be loaded.";
				i = i ? `${i} ${t}` : t;
			} finally {
				l.value = !1;
			}
			i ? (u.value = i, e || r("notify", i, "error")) : e || (d.value = "Redis status refreshed.", u.value = "");
		}
		async function w() {
			let e = b();
			if (e) {
				x(e, "error");
				return;
			}
			c.value = "test", u.value = "", d.value = "Testing Redis connection…";
			try {
				await js(n.configureEndpoint, y(!0)), x("Redis connection test succeeded.");
			} catch (e) {
				x(e instanceof Error ? `Redis test failed: ${e.message}` : "Redis connection test failed.", "error");
			} finally {
				c.value = "";
			}
		}
		async function T() {
			let e = b();
			if (e) {
				x(e, "error");
				return;
			}
			c.value = "save", u.value = "", d.value = "Saving Redis settings…";
			try {
				let e = await js(n.configureEndpoint, y(!1));
				s.value = !1, _(e, !0), x(`Redis settings saved.${S(e.bootstrap_replay)}`), await C(!0, !0);
			} catch (e) {
				x(e instanceof Error ? `Redis save failed: ${e.message}` : "Redis settings could not be saved.", "error");
			} finally {
				c.value = "";
			}
		}
		async function E() {
			if (!j.value) return;
			let e = p(i.data_path) || "the default internal Redis store";
			if (window.confirm(`Migrate the connected external Redis database into ${e} and switch Tater to internal Redis? The target internal DB will be replaced.`)) {
				c.value = "migrate", u.value = "", d.value = "Pausing active runtimes and migrating Redis data…";
				try {
					let e = await js(n.migrateEndpoint, {
						data_path: p(i.data_path),
						flush_internal: !0
					});
					s.value = !1, _(e.redis_status || e, !0), e.encryption_status && (o.value = h(e.encryption_status));
					let t = e.migration && typeof e.migration == "object" ? e.migration : {};
					x(`Migrated ${Number(t.keys_restored || 0)} Redis key(s) and switched to internal Redis.${S(e.bootstrap_replay)}`), await C(!0, !0);
				} catch (e) {
					x(e instanceof Error ? `Redis migration failed: ${e.message}` : "Redis migration failed.", "error");
				} finally {
					c.value = "";
				}
			}
		}
		async function D() {
			c.value = "encrypt", u.value = "", d.value = "Pausing active runtimes and encrypting Redis values…";
			try {
				let e = await js(n.encryptEndpoint);
				o.value = h(e.encryption_status);
				let t = e.key_created ? " A new encryption key was generated." : "";
				x(`Encrypted ${Number(e.keys_encrypted || 0)} Redis value(s); live encryption is enabled.${t}`), await C(!0, !0);
			} catch (e) {
				x(e instanceof Error ? `Redis encryption failed: ${e.message}` : "Redis encryption failed.", "error");
			} finally {
				c.value = "";
			}
		}
		async function O() {
			if (window.confirm("Decrypt live Redis values now? Future writes will return to plaintext.")) {
				c.value = "decrypt", u.value = "", d.value = "Pausing active runtimes and decrypting Redis values…";
				try {
					let e = await js(n.decryptEndpoint);
					o.value = h(e.encryption_status), x(`Decrypted ${Number(e.restored_keys || 0)} Redis value(s); live encryption is disabled.${S(e.bootstrap_replay)}`), await C(!0, !0);
				} catch (e) {
					x(e instanceof Error ? `Redis decrypt failed: ${e.message}` : "Redis decryption failed.", "error");
				} finally {
					c.value = "";
				}
			}
		}
		let k = Q(() => i.mode === "internal"), A = Q(() => !!a.value.connected), j = Q(() => A.value && !a.value.internal && a.value.mode === "external" && !c.value), M = Q(() => !!o.value.live_encryption_enabled), N = Q(() => o.value.encryption_available !== !1 && !o.value.error), P = Q(() => a.value.internal ? A.value ? a.value.data_path ? `Internal Redis is connected at ${a.value.data_path}.` : "Internal Redis is connected." : a.value.error ? `Internal Redis could not start: ${a.value.error}` : "Internal Redis is not running." : a.value.configured ? A.value ? `External Redis is connected at ${a.value.host}:${a.value.port}.` : a.value.error ? `External Redis is unavailable: ${a.value.error}` : "External Redis is unavailable." : "External Redis is not configured yet."), F = Q(() => N.value ? M.value ? `Live Redis encryption is enabled.${o.value.key_fingerprint ? ` Key ${o.value.key_fingerprint}.` : ""}` : o.value.key_exists ? o.value.key_fingerprint ? `Encryption key ready (${o.value.key_fingerprint}). Live encryption is disabled.` : "Encryption key ready. Live encryption is disabled." : "No encryption key exists yet. Encrypting live Redis will create one automatically." : p(o.value.error) || "Redis encryption tools are unavailable."), I = Q(() => a.value.internal ? "Internal" : p(a.value.host) || "External");
		return On(() => n.initialStatus, (e) => _(e || {}, !s.value), { immediate: !0 }), On(() => n.initialEncryptionStatus, (e) => {
			o.value = h(e);
		}, { immediate: !0 }), Er(() => {
			C(!0), f = window.setInterval(() => {
				document.visibilityState === "visible" && !c.value && C(!0);
			}, 1e4);
		}), kr(() => {
			f !== null && window.clearInterval(f);
		}), (e, t) => (q(), J("section", kP, [
			Y("section", AP, [t[12] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Live data service"),
				Y("h2", null, "Redis connection and encryption"),
				Y("p", null, "Connection health refreshes automatically while unsaved form edits stay untouched.")
			], -1), Y("div", jP, [Y("span", { class: B(["tv-live-pill", { warning: !A.value }]) }, [t[11] ||= Y("i", null, null, -1), X(V(l.value ? "Updating" : A.value ? "Connected" : "Needs attention"), 1)], 2), Y("button", {
				class: "tv-button",
				type: "button",
				disabled: !!c.value || l.value,
				onClick: t[0] ||= (e) => C(!1)
			}, "Refresh", 8, MP)])]),
			Y("div", NP, [
				Y("article", null, [t[13] ||= Y("span", null, "Mode", -1), Y("strong", null, V(k.value ? "Internal" : "External"), 1)]),
				Y("article", null, [t[14] ||= Y("span", null, "Connection", -1), Y("strong", null, V(A.value ? "Healthy" : "Unavailable"), 1)]),
				Y("article", null, [t[15] ||= Y("span", null, "Store", -1), Y("strong", null, V(I.value), 1)]),
				Y("article", null, [t[16] ||= Y("span", null, "Encryption", -1), Y("strong", null, V(M.value ? "Enabled" : "Disabled"), 1)])
			]),
			d.value || u.value ? (q(), J("div", {
				key: 0,
				class: B(["tv-notice", { error: !!u.value }]),
				"aria-live": "polite"
			}, V(u.value || d.value), 3)) : Z("", !0),
			a.value.fallback_reason ? (q(), J("div", PP, " Redis recovered to the internal store: " + V(a.value.fallback_reason), 1)) : Z("", !0),
			Y("div", FP, [
				Y("section", IP, [
					Y("header", LP, [Y("div", null, [
						t[17] ||= Y("span", { class: "tv-eyebrow" }, "Connection", -1),
						t[18] ||= Y("h2", null, "Redis server", -1),
						Y("p", null, V(P.value), 1)
					]), Y("span", { class: B(["tredis-state", { connected: A.value }]) }, [t[19] ||= Y("i", null, null, -1), X(V(A.value ? "Live" : "Offline"), 1)], 2)]),
					Y("div", RP, [
						Y("label", null, [t[21] ||= X(" Mode ", -1), W(Y("select", {
							"onUpdate:modelValue": t[1] ||= (e) => i.mode = e,
							disabled: !!c.value,
							onChange: v
						}, [...t[20] ||= [Y("option", { value: "internal" }, "Internal", -1), Y("option", { value: "external" }, "External", -1)]], 40, zP), [[ds, i.mode]])]),
						k.value ? (q(), J("label", BP, [t[22] ||= X(" Internal data file ", -1), W(Y("input", {
							"onUpdate:modelValue": t[2] ||= (e) => i.data_path = e,
							type: "text",
							disabled: !!c.value,
							onInput: v
						}, null, 40, VP), [[$, i.data_path]])])) : (q(), J(K, { key: 1 }, [
							Y("label", null, [t[23] ||= X(" Host ", -1), W(Y("input", {
								"onUpdate:modelValue": t[3] ||= (e) => i.host = e,
								type: "text",
								disabled: !!c.value,
								onInput: v
							}, null, 40, HP), [[$, i.host]])]),
							Y("label", null, [t[24] ||= X(" Port ", -1), W(Y("input", {
								"onUpdate:modelValue": t[4] ||= (e) => i.port = e,
								type: "number",
								min: "1",
								max: "65535",
								disabled: !!c.value,
								onInput: v
							}, null, 40, UP), [[
								$,
								i.port,
								void 0,
								{ number: !0 }
							]])]),
							Y("label", null, [
								t[25] ||= X(" Username ", -1),
								t[26] ||= Y("small", null, "Optional", -1),
								W(Y("input", {
									"onUpdate:modelValue": t[5] ||= (e) => i.username = e,
									type: "text",
									autocomplete: "username",
									disabled: !!c.value,
									onInput: v
								}, null, 40, WP), [[$, i.username]])
							]),
							Y("label", null, [
								t[27] ||= X(" Password ", -1),
								Y("small", null, V(a.value.password_set ? "Leave blank to keep the saved password" : "Optional"), 1),
								W(Y("input", {
									"onUpdate:modelValue": t[6] ||= (e) => i.password = e,
									type: "password",
									autocomplete: "new-password",
									placeholder: a.value.password_set ? "Saved password will be kept" : "",
									disabled: !!c.value,
									onInput: v
								}, null, 40, GP), [[$, i.password]])
							])
						], 64)),
						Y("label", null, [t[28] ||= X(" Database ", -1), W(Y("input", {
							"onUpdate:modelValue": t[7] ||= (e) => i.db = e,
							type: "number",
							min: "0",
							disabled: !!c.value,
							onInput: v
						}, null, 40, KP), [[
							$,
							i.db,
							void 0,
							{ number: !0 }
						]])]),
						k.value ? Z("", !0) : (q(), J(K, { key: 2 }, [
							Y("label", qP, [W(Y("input", {
								"onUpdate:modelValue": t[8] ||= (e) => i.use_tls = e,
								class: "tv-checkbox",
								type: "checkbox",
								disabled: !!c.value,
								onChange: v
							}, null, 40, JP), [[cs, i.use_tls]]), t[29] ||= Y("span", null, [Y("strong", null, "Use TLS"), Y("small", null, "Encrypt the Redis network connection.")], -1)]),
							Y("label", YP, [W(Y("input", {
								"onUpdate:modelValue": t[9] ||= (e) => i.verify_tls = e,
								class: "tv-checkbox",
								type: "checkbox",
								disabled: !!c.value || !i.use_tls,
								onChange: v
							}, null, 40, XP), [[cs, i.verify_tls]]), t[30] ||= Y("span", null, [Y("strong", null, "Verify TLS certificate"), Y("small", null, "Recommended for external Redis.")], -1)]),
							i.use_tls ? (q(), J("label", ZP, [
								t[31] ||= X(" CA certificate path ", -1),
								t[32] ||= Y("small", null, "Optional", -1),
								W(Y("input", {
									"onUpdate:modelValue": t[10] ||= (e) => i.ca_cert_path = e,
									type: "text",
									disabled: !!c.value,
									onInput: v
								}, null, 40, QP), [[$, i.ca_cert_path]])
							])) : Z("", !0)
						], 64))
					]),
					Y("div", $P, [Y("button", {
						class: "tv-button",
						type: "button",
						disabled: !!c.value,
						onClick: w
					}, V(c.value === "test" ? "Testing…" : "Test Connection"), 9, eF), Y("button", {
						class: "tv-button",
						type: "button",
						disabled: !j.value,
						title: a.value.internal ? "Redis is already internal." : A.value ? "" : "Connect external Redis before migrating.",
						onClick: E
					}, V(c.value === "migrate" ? "Migrating…" : "Migrate to Internal"), 9, tF)])
				]),
				Y("section", nF, [
					Y("header", rF, [Y("div", null, [
						t[33] ||= Y("span", { class: "tv-eyebrow" }, "At-rest protection", -1),
						t[34] ||= Y("h2", null, "Live value encryption", -1),
						Y("p", null, V(F.value), 1)
					]), Y("span", { class: B(["tredis-state", { connected: M.value }]) }, [t[35] ||= Y("i", null, null, -1), X(V(M.value ? "Encrypted" : "Plaintext"), 1)], 2)]),
					Y("dl", iF, [
						Y("div", null, [t[36] ||= Y("dt", null, "Key file", -1), Y("dd", null, V(o.value.key_path || "Not available"), 1)]),
						Y("div", null, [t[37] ||= Y("dt", null, "Mode state file", -1), Y("dd", null, V(o.value.live_encryption_state_path || "Not available"), 1)]),
						Y("div", null, [t[38] ||= Y("dt", null, "Last updated", -1), Y("dd", null, V(o.value.live_encryption_updated || "Not recorded"), 1)]),
						Y("div", null, [t[39] ||= Y("dt", null, "Key fingerprint", -1), Y("dd", null, V(o.value.key_fingerprint || "Not created"), 1)])
					]),
					t[40] ||= Y("p", { class: "tredis-help" }, "Encrypt transforms existing values in place, creates a key when needed, and encrypts future writes. Decrypt reverses the values and returns future writes to plaintext.", -1),
					Y("div", aF, [Y("button", {
						class: "tv-button",
						type: "button",
						disabled: !!c.value || !A.value || !N.value || M.value,
						onClick: D
					}, V(c.value === "encrypt" ? "Encrypting…" : "Encrypt Live Redis"), 9, oF), Y("button", {
						class: "tv-button danger",
						type: "button",
						disabled: !!c.value || !A.value || !N.value || !M.value,
						onClick: O
					}, V(c.value === "decrypt" ? "Decrypting…" : "Decrypt Live Redis"), 9, sF)])
				]),
				Y("section", cF, [t[47] ||= Y("header", null, [
					Y("span", { class: "tv-eyebrow" }, "Runtime details"),
					Y("h2", null, "Storage health"),
					Y("p", null, "Useful paths and process details for recovery and diagnostics.")
				], -1), Y("dl", lF, [
					Y("div", null, [t[41] ||= Y("dt", null, "Configuration source", -1), Y("dd", null, V(a.value.source || "Unknown"), 1)]),
					Y("div", null, [t[42] ||= Y("dt", null, "Configuration file", -1), Y("dd", null, V(a.value.config_path || "Not reported"), 1)]),
					Y("div", null, [t[43] ||= Y("dt", null, "Data directory", -1), Y("dd", null, V(a.value.data_dir || "External service"), 1)]),
					Y("div", null, [t[44] ||= Y("dt", null, "Socket", -1), Y("dd", null, V(a.value.socket_path || "TCP connection"), 1)]),
					Y("div", null, [t[45] ||= Y("dt", null, "Managed process", -1), Y("dd", null, V(a.value.redis_managed ? "Yes" : "No"), 1)]),
					Y("div", null, [t[46] ||= Y("dt", null, "Process ID", -1), Y("dd", null, V(a.value.redis_pid || "Not reported"), 1)])
				])])
			]),
			Y("section", uF, [Y("div", null, [Y("strong", null, V(s.value ? "Unsaved Redis changes" : "Redis settings are synchronized"), 1), t[48] ||= Y("span", null, "Live health continues to refresh without replacing edits in progress.", -1)]), Y("button", {
				class: "tv-button primary",
				type: "button",
				disabled: !!c.value || !s.value,
				onClick: T
			}, V(c.value === "save" ? "Saving…" : "Save Redis Settings"), 9, dF)])
		]));
	}
}), pF = { class: "tset-resource tlink-resource" }, mF = { class: "tv-panel tlink-hero" }, hF = { class: "tlink-live" }, gF = {
	class: "tv-tabs tlink-tabs",
	"aria-label": "Spud Link sections"
}, _F = { class: "tlink-pair-grid" }, vF = { class: "tv-panel tlink-pair-card" }, yF = ["disabled"], bF = { class: "tv-panel tlink-pair-card" }, xF = ["disabled"], SF = { class: "tv-panel tlink-nodes" }, CF = { class: "tv-panel-head" }, wF = { class: "tlink-head-actions" }, TF = { class: "tv-live-pill" }, EF = ["disabled"], DF = {
	key: 0,
	class: "tlink-node-list"
}, OF = { class: "tlink-facts" }, kF = { key: 0 }, AF = { key: 1 }, jF = { key: 2 }, MF = ["disabled", "onClick"], NF = {
	key: 1,
	class: "tv-empty"
}, PF = { class: "tv-panel tset-form-card tlink-connect" }, FF = { class: "tv-form-grid" }, IF = { class: "full" }, LF = { class: "full" }, RF = { class: "tlink-actions" }, zF = ["disabled"], BF = ["disabled"], VF = { class: "tv-panel tlink-routing" }, HF = { class: "tv-panel-head" }, UF = { class: "tv-toggle compact" }, WF = { class: "tlink-route-grid" }, GF = { key: 0 }, KF = ["onUpdate:modelValue", "disabled"], qF = { class: "tlink-settings-grid" }, JF = { class: "tv-panel tset-form-card" }, YF = { class: "tv-form-grid" }, XF = { class: "tlink-mode-help" }, ZF = { class: "tv-panel tset-form-card" }, QF = { class: "tlink-toggle-grid" }, $F = { class: "tv-toggle" }, eI = { class: "tv-toggle" }, tI = { class: "tv-toggle" }, nI = { class: "tv-toggle" }, rI = { class: "tv-toggle" }, iI = { class: "tv-toggle" }, aI = { class: "tv-panel tlink-endpoints" }, oI = {
	key: 4,
	class: "tset-save-bar"
}, sI = ["disabled"], cI = {
	class: "tlink-modal",
	role: "dialog",
	"aria-modal": "true",
	"aria-labelledby": "tlink-pair-title"
}, lI = { id: "tlink-pair-title" }, uI = {
	key: 0,
	class: "tlink-modal-state"
}, dI = {
	key: 1,
	class: "tlink-modal-state success"
}, fI = {
	key: 2,
	class: "tlink-modal-state error"
}, pI = {
	key: 3,
	class: "tlink-modal-state"
}, mI = ["src"], hI = {
	key: 1,
	class: "tlink-code"
}, gI = { key: 0 }, _I = /* @__PURE__ */ sr({
	__name: "SpudLinkSettings",
	props: {
		settings: {},
		endpoint: {},
		statusEndpoint: {},
		pairingCodeEndpoint: {},
		connectEndpoint: {},
		revokeEndpoint: {},
		llmApiUrl: {},
		modelsApiUrl: {},
		pairApiUrl: {},
		initialTab: {}
	},
	emits: [
		"changed",
		"tabChange",
		"notify"
	],
	setup(e, { emit: t }) {
		let n = e, r = t, i = [
			{
				id: "llm",
				label: "LLM",
				note: "Hydra planning, tools, chat, and final answers"
			},
			{
				id: "vad",
				label: "Speech-End Detection (VAD)",
				note: "The Hub detects when speech ends; this Tater keeps a local fallback"
			},
			{
				id: "stt",
				label: "Speech to Text",
				note: "Transcription runs on the Hub"
			},
			{
				id: "tts",
				label: "Text to Speech",
				note: "The Hub returns ready-to-play speech audio"
			},
			{
				id: "vision",
				label: "Vision",
				note: "Image descriptions and camera snapshots"
			},
			{
				id: "audio",
				label: "Audio Understanding",
				note: "Music and general audio analysis"
			},
			{
				id: "video",
				label: "Video Understanding",
				note: "Camera clips and attached video analysis"
			},
			{
				id: "speaker_id",
				label: "Speaker ID",
				note: "Voice embeddings run on the Hub; profiles stay here"
			},
			{
				id: "emotion_id",
				label: "Emotion ID",
				note: "Voice tone analysis"
			},
			{
				id: "face_id",
				label: "Face ID",
				note: "Face embeddings run on the Hub; People and the face library stay here"
			}
		], a = /* @__PURE__ */ new Set([
			"pair",
			"spudlet",
			"settings"
		]), o = (e) => a.has(String(e || "").trim()) ? String(e).trim() : "pair", s = /* @__PURE__ */ U(o(n.initialTab)), c = /* @__PURE__ */ U(!1), l = /* @__PURE__ */ U(!1), u = /* @__PURE__ */ U(!1), d = /* @__PURE__ */ U(!1), f = /* @__PURE__ */ U(""), p = /* @__PURE__ */ U(""), m = /* @__PURE__ */ U(""), h = /* @__PURE__ */ U(""), g = /* @__PURE__ */ Et({
			mode: "disabled",
			node_name: "Tater",
			home_url: "",
			public_url: "",
			pairing_enabled: !1,
			allow_spudlets: !0,
			allow_little_spuds: !0,
			little_spud_tools_enabled: !0,
			telemetry_enabled: !0,
			request_previews_enabled: !1,
			model_routing_enabled: !1,
			hub_url: "",
			routes: {}
		}), _ = /* @__PURE__ */ Et({
			open: !1,
			role: "little_spud",
			phase: "idle",
			message: "",
			code: "",
			qr: "",
			expiresAt: 0,
			startedAt: 0,
			connectedName: ""
		}), v = 0, y = 0, b = !1, x = Q(() => Array.isArray(n.settings.linked_nodes) ? n.settings.linked_nodes : []), S = Q(() => n.settings.paired_hub && typeof n.settings.paired_hub == "object" ? n.settings.paired_hub : {}), C = Q(() => !!S.value.connected), w = Q(() => _.phase === "loading" || _.phase === "waiting"), T = Q(() => g.mode === "hub" ? "Spud Hub" : g.mode === "spudlet" ? "Spudlet" : "Disabled"), E = Q(() => g.mode === "hub" ? "This Tater accepts Little Spuds and Spudlets and shares its models, Hydra, tools, and memory." : g.mode === "spudlet" ? "This Tater uses an upstream Spud Hub while keeping its own app and local data." : "Spud Link is off. Linked clients cannot pair or make remote requests until it is enabled.");
		function D(e) {
			if (e === "llm" || g.model_routing_enabled) return "hub";
			let t = String(g.routes[e] || "auto").toLowerCase();
			return [
				"auto",
				"hub",
				"local"
			].includes(t) ? t : "auto";
		}
		function O(e) {
			if (g.mode !== "spudlet" || !C.value) return !1;
			let t = D(e);
			return t !== "local" && (e === "llm" || t === "hub" || g.model_routing_enabled);
		}
		function k(e) {
			let t = e.model_routing && typeof e.model_routing == "object" ? e.model_routing : {}, n = e.model_routes && typeof e.model_routes == "object" ? e.model_routes : t.routes && typeof t.routes == "object" ? t.routes : {};
			Object.assign(g, {
				mode: ["hub", "spudlet"].includes(String(e.mode || "")) ? String(e.mode) : "disabled",
				node_name: String(e.node_name || "Tater"),
				home_url: String(e.home_url || window.location.origin || ""),
				public_url: String(e.public_url || ""),
				pairing_enabled: !!e.pairing_enabled,
				allow_spudlets: e.allow_spudlets !== !1,
				allow_little_spuds: e.allow_little_spuds !== !1,
				little_spud_tools_enabled: e.little_spud_tools_enabled !== !1,
				telemetry_enabled: e.telemetry_enabled !== !1,
				request_previews_enabled: !!e.request_previews_enabled,
				model_routing_enabled: !!(e.model_routing_enabled ?? t.enabled),
				hub_url: String(e.hub_url || "")
			}), g.routes = Object.fromEntries(i.map((e) => [e.id, e.id === "llm" ? "hub" : String(n[e.id] || "auto")])), d.value = !1;
		}
		function A() {
			d.value = !0, f.value = "", p.value = "";
		}
		function j(e) {
			s.value = o(e), r("tabChange", s.value);
		}
		function M(e = {}) {
			return {
				spud_link_mode: g.mode,
				spud_link_node_name: g.node_name.trim(),
				spud_link_home_url: g.home_url.trim().replace(/\/+$/, ""),
				spud_link_public_url: g.public_url.trim().replace(/\/+$/, ""),
				spud_link_pairing_enabled: g.pairing_enabled,
				spud_link_allow_spudlets: g.allow_spudlets,
				spud_link_allow_little_spuds: g.allow_little_spuds,
				spud_link_little_spud_tools_enabled: g.little_spud_tools_enabled,
				spud_link_telemetry_enabled: g.telemetry_enabled,
				spud_link_request_previews_enabled: g.request_previews_enabled,
				spud_link_model_routing_enabled: g.model_routing_enabled,
				...Object.fromEntries(i.map((e) => [`spud_link_model_route_${e.id}`, D(e.id)])),
				spud_link_hub_url: g.hub_url.trim().replace(/\/+$/, ""),
				...e
			};
		}
		async function N(e = {}) {
			let t = await js(n.endpoint, M(e));
			return r("changed", t), k(t), t;
		}
		async function P() {
			c.value = !0, f.value = "", p.value = "";
			try {
				await N(), p.value = "Spud Link settings saved and synchronized.", r("notify", p.value, "success");
			} catch (e) {
				f.value = e instanceof Error ? e.message : "Spud Link settings could not be saved.", r("notify", f.value, "error");
			} finally {
				c.value = !1;
			}
		}
		async function F(e = !1) {
			if (!(u.value || b)) {
				u.value = !0;
				try {
					let t = await As(n.statusEndpoint), i = t.spud_link && typeof t.spud_link == "object" ? t.spud_link : null;
					i && (r("changed", i), d.value || k(i)), e || (p.value = "Spud Link status refreshed.");
				} catch (t) {
					e || (f.value = t instanceof Error ? t.message : "Spud Link status could not be refreshed.", r("notify", f.value, "error"));
				} finally {
					u.value = !1;
				}
			}
		}
		function I() {
			window.clearTimeout(v), !b && (v = window.setTimeout(async () => {
				await F(!0), I();
			}, 5e3));
		}
		function ee(e, t, n = !1) {
			let r = e.trim().replace(/\/+$/, "");
			if (!r) {
				if (n) throw Error(`Enter the ${t} before creating the QR code.`);
				return "";
			}
			let i;
			try {
				i = new URL(r);
			} catch {
				throw Error(`${t} must be a complete http:// or https:// address.`);
			}
			if (!["http:", "https:"].includes(i.protocol) || !i.hostname) throw Error(`${t} must be a complete http:// or https:// address.`);
			return r;
		}
		function te() {
			window.clearTimeout(y), y = 0, _.open = !1, _.phase = "idle";
		}
		async function ne() {
			if (!(!_.open || _.phase !== "waiting" || b)) {
				try {
					let e = await As(n.statusEndpoint), t = e.spud_link && typeof e.spud_link == "object" ? e.spud_link : {};
					r("changed", t), d.value || k(t);
					let i = Array.isArray(t.linked_nodes) ? t.linked_nodes.find((e) => String(e.role || "").toLowerCase() === _.role && Math.max(Number(e.created_at || 0), Number(e.last_seen_at || 0)) >= _.startedAt - 1) : null;
					if (i && !t.pairing_code_active) {
						_.phase = "success", _.connectedName = String(i.name || (_.role === "spudlet" ? "Spudlet" : "Little Spud")), _.message = `${_.connectedName} is connected and ready.`, r("notify", _.role === "spudlet" ? "Spudlet connected." : "Little Spud connected.", "success");
						return;
					}
				} catch {}
				y = window.setTimeout(ne, 1100);
			}
		}
		async function L(e) {
			f.value = "";
			let t = "", i = "";
			try {
				e === "little_spud" && (t = ee(g.home_url, "Home / LAN URL", !0), i = ee(g.public_url, "Away / Tater Tunnel URL"));
			} catch (e) {
				f.value = e instanceof Error ? e.message : "Enter valid connection addresses.", r("notify", f.value, "error");
				return;
			}
			_.open = !0, _.role = e, _.phase = "loading", _.message = e === "spudlet" ? "Creating a short code for the other Tater…" : "Creating a private QR code for Little Spud…", _.code = "", _.qr = "", _.connectedName = "", _.startedAt = Date.now() / 1e3;
			try {
				await N({
					spud_link_mode: g.mode === "disabled" ? "hub" : g.mode,
					spud_link_pairing_enabled: !0,
					...e === "spudlet" ? { spud_link_allow_spudlets: !0 } : { spud_link_allow_little_spuds: !0 },
					...e === "little_spud" ? {
						spud_link_home_url: t,
						spud_link_public_url: i
					} : {}
				});
				let r = await js(n.pairingCodeEndpoint, {
					role: e,
					...e === "little_spud" ? {
						home_url: t,
						public_url: i
					} : {}
				});
				if (_.code = String(r.manual_code || r.pairing_code || ""), _.qr = String(r.pairing_qr_svg || ""), _.expiresAt = Number(r.expires_at || 0), e === "little_spud" && !_.qr) throw Error("QR generation is unavailable on this Tater install.");
				_.phase = "waiting", _.message = e === "spudlet" ? "Paste this code into the other Tater." : "Open Little Spud and scan this QR code.", ne();
			} catch (e) {
				_.phase = "error", _.message = e instanceof Error ? e.message : "The pairing invitation could not be created.", r("notify", `Pairing failed: ${_.message}`, "error");
			}
		}
		async function R() {
			if (_.code) try {
				await navigator.clipboard.writeText(_.code), p.value = "Spudlet pairing code copied.";
			} catch (e) {
				f.value = e instanceof Error ? e.message : "The pairing code could not be copied.";
			}
		}
		async function z() {
			let e = g.hub_url.trim(), t = m.value.trim();
			if (!e || !t) {
				f.value = "Spud Hub URL and pairing code are required.", r("notify", f.value, "error");
				return;
			}
			l.value = !0, f.value = "", p.value = "";
			try {
				await js(n.connectEndpoint, {
					hub_url: e,
					pairing_code: t,
					role: "spudlet",
					node_name: g.node_name.trim(),
					public_url: g.public_url.trim()
				}), m.value = "";
				let i = await As(n.endpoint);
				r("changed", i), k(i), p.value = "Connected to the Spud Hub. Model routes are live now.", r("notify", p.value, "success");
			} catch (e) {
				f.value = e instanceof Error ? e.message : "The Spud Hub connection failed.", r("notify", `Connect failed: ${f.value}`, "error");
			} finally {
				l.value = !1;
			}
		}
		async function re() {
			if (window.confirm("Disconnect this Tater from its Spud Hub? It will need a new pairing code to reconnect.")) {
				c.value = !0;
				try {
					await N({
						clear_spud_link_node_token: !0,
						spud_link_mode: "disabled"
					}), p.value = "This Tater is disconnected from its Spud Hub.", r("notify", p.value, "success");
				} catch (e) {
					f.value = e instanceof Error ? e.message : "The Hub connection could not be removed.", r("notify", f.value, "error");
				} finally {
					c.value = !1;
				}
			}
		}
		async function ie(e) {
			let t = String(e.id || "").trim(), i = String(e.name || t || "this linked Spud");
			if (!(!t || !window.confirm(`Revoke ${i}? This device will need to pair again.`))) {
				h.value = t, f.value = "";
				try {
					await js(n.revokeEndpoint, { node_id: t }), await F(!0), p.value = `${i} revoked.`, r("notify", p.value, "success");
				} catch (e) {
					f.value = e instanceof Error ? e.message : "The linked Spud could not be revoked.", r("notify", `Revoke failed: ${f.value}`, "error");
				} finally {
					h.value = "";
				}
			}
		}
		function ae(e) {
			return String(e || "") === "little_spud" ? "Little Spud" : String(e || "") === "spudlet" ? "Spudlet" : "Linked Spud";
		}
		function oe(e) {
			let t = Number(e || 0);
			return t > 0 ? (/* @__PURE__ */ new Date(t * 1e3)).toLocaleString() : "Never";
		}
		return On(() => n.settings, (e) => {
			d.value || k(e || {});
		}, { immediate: !0 }), Er(async () => {
			await F(!0), I();
		}), kr(() => {
			b = !0, window.clearTimeout(v), window.clearTimeout(y);
		}), (t, n) => (q(), J("section", pF, [
			p.value || f.value ? (q(), J("div", {
				key: 0,
				class: B(["tv-notice", { error: !!f.value }]),
				"aria-live": "polite"
			}, V(f.value || p.value), 3)) : Z("", !0),
			Y("section", mF, [n[19] ||= ba("<div class=\"tlink-orbit\" aria-hidden=\"true\"><i class=\"hub\"></i><i class=\"spudlet\"></i><i class=\"little\"></i></div><div><span class=\"tv-eyebrow\">One Tater, three ways to connect</span><h2>Spud Link</h2><p>Share models and tools from a Hub, borrow them as a Spudlet, or pair a lightweight Little Spud by QR.</p></div>", 2), Y("div", hF, [Y("span", { class: B({ connected: C.value || x.value.length > 0 }) }, null, 2), X(" " + V(C.value ? "Hub connected" : x.value.length ? `${x.value.length} linked` : T.value), 1)])]),
			Y("nav", gF, [
				Y("button", {
					type: "button",
					class: B({ active: s.value === "pair" }),
					onClick: n[0] ||= (e) => j("pair")
				}, "Pair devices", 2),
				Y("button", {
					type: "button",
					class: B({ active: s.value === "spudlet" }),
					onClick: n[1] ||= (e) => j("spudlet")
				}, "Use a Spud Hub", 2),
				Y("button", {
					type: "button",
					class: B({ active: s.value === "settings" }),
					onClick: n[2] ||= (e) => j("settings")
				}, "Settings", 2)
			]),
			s.value === "pair" ? (q(), J(K, { key: 1 }, [Y("div", _F, [Y("section", vF, [
				n[24] ||= Y("header", null, [
					Y("span", { class: "tv-eyebrow" }, "Little Spud"),
					Y("h2", null, "Pair by QR"),
					Y("p", null, "Create one private code containing the addresses this companion should use.")
				], -1),
				Y("label", null, [
					n[20] ||= X("Home / LAN URL", -1),
					W(Y("input", {
						"onUpdate:modelValue": n[3] ||= (e) => g.home_url = e,
						type: "url",
						placeholder: "http://tater.local:8501",
						onInput: A
					}, null, 544), [[$, g.home_url]]),
					n[21] ||= Y("small", null, "Required. Used while the Little Spud is on your home network.", -1)
				]),
				Y("label", null, [
					n[22] ||= X("Away / Tater Tunnel URL", -1),
					W(Y("input", {
						"onUpdate:modelValue": n[4] ||= (e) => g.public_url = e,
						type: "url",
						placeholder: "https://your-tater-tunnel.example",
						onInput: A
					}, null, 544), [[$, g.public_url]]),
					n[23] ||= Y("small", null, "Optional fallback for use away from home.", -1)
				]),
				Y("button", {
					class: "tv-button primary",
					type: "button",
					disabled: w.value,
					onClick: n[5] ||= (e) => L("little_spud")
				}, "Show Little Spud QR", 8, yF)
			]), Y("section", bF, [n[25] ||= ba("<header><span class=\"tv-eyebrow\">Spudlet</span><h2>Link another Tater</h2><p>Create a short, single-use code to paste into another full Tater.</p></header><div class=\"tlink-pair-steps\"><strong>1</strong><span>Open Spud Link on the other Tater.</span><strong>2</strong><span>Paste the Hub URL and code there.</span><strong>3</strong><span>The permanent token is saved automatically.</span></div>", 2), Y("button", {
				class: "tv-button primary",
				type: "button",
				disabled: w.value,
				onClick: n[6] ||= (e) => L("spudlet")
			}, "Create Spudlet code", 8, xF)])]), Y("section", SF, [Y("header", CF, [n[27] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Linked Spuds"),
				Y("h2", null, "Devices connected to this Tater"),
				Y("p", null, "Live status refreshes every five seconds.")
			], -1), Y("div", wF, [Y("span", TF, [n[26] ||= Y("i", null, null, -1), X(V(x.value.length) + " linked", 1)]), Y("button", {
				class: "tv-button",
				type: "button",
				disabled: u.value,
				onClick: n[7] ||= (e) => F()
			}, V(u.value ? "Refreshing…" : "Refresh"), 9, EF)])]), x.value.length ? (q(), J("div", DF, [(q(!0), J(K, null, G(x.value, (e) => (q(), J("article", {
				key: String(e.id || e.name),
				class: "tlink-node-row"
			}, [
				n[28] ||= Y("span", { class: "tlink-node-dot" }, null, -1),
				Y("div", null, [
					Y("strong", null, V(e.name || e.id || "Linked Spud"), 1),
					Y("small", null, V(ae(e.role)) + " · last seen " + V(oe(e.last_seen_at)), 1),
					Y("div", OF, [
						e.last_remote_addr ? (q(), J("span", kF, "IP " + V(e.last_remote_addr), 1)) : Z("", !0),
						e.version ? (q(), J("span", AF, "v" + V(e.version), 1)) : Z("", !0),
						e.remote_mode ? (q(), J("span", jF, V(e.remote_mode), 1)) : Z("", !0)
					])
				]),
				Y("button", {
					class: "tv-button danger",
					type: "button",
					disabled: h.value === String(e.id || ""),
					onClick: (t) => ie(e)
				}, V(h.value === String(e.id || "") ? "Revoking…" : "Revoke"), 9, MF)
			]))), 128))])) : (q(), J("div", NF, "No linked Spuds yet. Create a QR or Spudlet code above to connect one."))])], 64)) : s.value === "spudlet" ? (q(), J(K, { key: 2 }, [Y("section", PF, [
				n[33] ||= Y("header", { class: "tv-panel-head" }, [Y("div", null, [
					Y("span", { class: "tv-eyebrow" }, "Upstream Hub"),
					Y("h2", null, "Connect this Tater to a Spud Hub"),
					Y("p", null, "Create a Spudlet code on the main Tater, then paste its address and code here.")
				]), Y("span", { class: "tlink-mode-chip" }, "Spudlet")], -1),
				Y("div", FF, [Y("label", IF, [
					n[29] ||= X("Spud Hub URL", -1),
					W(Y("input", {
						"onUpdate:modelValue": n[8] ||= (e) => g.hub_url = e,
						type: "text",
						placeholder: "http://spud-hub.local:8501",
						onInput: A
					}, null, 544), [[$, g.hub_url]]),
					n[30] ||= Y("small", null, "Use its LAN address at home or its public Tater Tunnel address.", -1)
				]), Y("label", LF, [n[31] ||= X("Spudlet pairing code", -1), W(Y("input", {
					"onUpdate:modelValue": n[9] ||= (e) => m.value = e,
					type: "text",
					autocomplete: "off",
					placeholder: "SPUD-XXXXXX-XXXXXX"
				}, null, 512), [[$, m.value]])])]),
				Y("div", { class: B(["tlink-connection", { connected: C.value }]) }, [n[32] ||= Y("span", { class: "tlink-node-dot" }, null, -1), Y("div", null, [Y("strong", null, V(C.value ? `Connected to ${S.value.hub_name || "Spud Hub"}` : "Not connected to a Spud Hub"), 1), Y("small", null, V(C.value ? `${S.value.hub_url || g.hub_url} · paired ${oe(S.value.connected_at)}` : "Connecting changes this Tater to Spudlet mode and saves its private token."), 1)])], 2),
				Y("div", RF, [Y("button", {
					class: "tv-button primary",
					type: "button",
					disabled: l.value,
					onClick: z
				}, V(l.value ? "Connecting…" : "Connect to Spud Hub"), 9, zF), C.value ? (q(), J("button", {
					key: 0,
					class: "tv-button danger",
					type: "button",
					disabled: c.value,
					onClick: re
				}, "Disconnect", 8, BF)) : Z("", !0)])
			]), Y("section", VF, [Y("header", HF, [n[35] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Model routing"),
				Y("h2", null, "Choose what runs on the Hub"),
				Y("p", null, "Wake word detection stays on this device. Other model work can run here or on the paired Hub.")
			], -1), Y("label", UF, [W(Y("input", {
				"onUpdate:modelValue": n[10] ||= (e) => g.model_routing_enabled = e,
				class: "tv-checkbox",
				type: "checkbox",
				onChange: A
			}, null, 544), [[cs, g.model_routing_enabled]]), n[34] ||= Y("span", null, [Y("strong", null, "Use Hub for all models"), Y("small", null, "Recommended on low-power installs")], -1)])]), Y("div", WF, [(q(), J(K, null, G(i, (e) => Y("label", {
				key: e.id,
				class: B(["tlink-route", { hub: O(e.id) }])
			}, [Y("span", null, [Y("strong", null, V(e.label), 1), Y("small", null, V(e.note), 1)]), Y("span", null, [e.id === "llm" ? (q(), J("em", GF, "Spud Hub")) : W((q(), J("select", {
				key: 1,
				"onUpdate:modelValue": (t) => g.routes[e.id] = t,
				disabled: g.model_routing_enabled,
				onChange: A
			}, [...n[36] ||= [
				Y("option", { value: "auto" }, "Auto", -1),
				Y("option", { value: "hub" }, "Spud Hub", -1),
				Y("option", { value: "local" }, "This Tater", -1)
			]], 40, KF)), [[ds, g.routes[e.id]]]), Y("small", null, V(O(e.id) ? "Loaded on Spud Hub" : "Runs on this Tater"), 1)])], 2)), 64))])])], 64)) : (q(), J(K, { key: 3 }, [Y("div", qF, [Y("section", JF, [
				n[41] ||= Y("header", null, [Y("span", { class: "tv-eyebrow" }, "Role"), Y("h2", null, "How this Tater uses Spud Link")], -1),
				Y("div", YF, [Y("label", null, [n[38] ||= X("Spud Link mode", -1), W(Y("select", {
					"onUpdate:modelValue": n[11] ||= (e) => g.mode = e,
					onChange: A
				}, [...n[37] ||= [
					Y("option", { value: "disabled" }, "Disabled", -1),
					Y("option", { value: "hub" }, "Spud Hub", -1),
					Y("option", { value: "spudlet" }, "Spudlet", -1)
				]], 544), [[ds, g.mode]])]), Y("label", null, [
					n[39] ||= X("Display name", -1),
					W(Y("input", {
						"onUpdate:modelValue": n[12] ||= (e) => g.node_name = e,
						type: "text",
						onInput: A
					}, null, 544), [[$, g.node_name]]),
					n[40] ||= Y("small", null, "Shown to linked devices and the upstream Hub.", -1)
				])]),
				Y("div", XF, [Y("strong", null, V(T.value), 1), Y("span", null, V(E.value), 1)])
			]), Y("section", ZF, [n[48] ||= Y("header", null, [Y("span", { class: "tv-eyebrow" }, "Pairing & privacy"), Y("h2", null, "Connection policy")], -1), Y("div", QF, [
				Y("label", $F, [W(Y("input", {
					"onUpdate:modelValue": n[13] ||= (e) => g.pairing_enabled = e,
					class: "tv-checkbox",
					type: "checkbox",
					onChange: A
				}, null, 544), [[cs, g.pairing_enabled]]), n[42] ||= Y("span", null, [Y("strong", null, "Allow new pairing invites"), Y("small", null, "Pair buttons enable this automatically.")], -1)]),
				Y("label", eI, [W(Y("input", {
					"onUpdate:modelValue": n[14] ||= (e) => g.allow_spudlets = e,
					class: "tv-checkbox",
					type: "checkbox",
					onChange: A
				}, null, 544), [[cs, g.allow_spudlets]]), n[43] ||= Y("span", null, [Y("strong", null, "Allow Spudlets"), Y("small", null, "Full Tater clients may connect.")], -1)]),
				Y("label", tI, [W(Y("input", {
					"onUpdate:modelValue": n[15] ||= (e) => g.allow_little_spuds = e,
					class: "tv-checkbox",
					type: "checkbox",
					onChange: A
				}, null, 544), [[cs, g.allow_little_spuds]]), n[44] ||= Y("span", null, [Y("strong", null, "Allow Little Spuds"), Y("small", null, "Lightweight companions may connect.")], -1)]),
				Y("label", nI, [W(Y("input", {
					"onUpdate:modelValue": n[16] ||= (e) => g.little_spud_tools_enabled = e,
					class: "tv-checkbox",
					type: "checkbox",
					onChange: A
				}, null, 544), [[cs, g.little_spud_tools_enabled]]), n[45] ||= Y("span", null, [Y("strong", null, "Little Spud tool use"), Y("small", null, "Allow paired companions to use Hydra tools.")], -1)]),
				Y("label", rI, [W(Y("input", {
					"onUpdate:modelValue": n[17] ||= (e) => g.telemetry_enabled = e,
					class: "tv-checkbox",
					type: "checkbox",
					onChange: A
				}, null, 544), [[cs, g.telemetry_enabled]]), n[46] ||= Y("span", null, [Y("strong", null, "Telemetry"), Y("small", null, "Keep linked-device health and activity metadata.")], -1)]),
				Y("label", iI, [W(Y("input", {
					"onUpdate:modelValue": n[18] ||= (e) => g.request_previews_enabled = e,
					class: "tv-checkbox",
					type: "checkbox",
					onChange: A
				}, null, 544), [[cs, g.request_previews_enabled]]), n[47] ||= Y("span", null, [Y("strong", null, "Request previews"), Y("small", null, "Off by default so activity remains metadata-first.")], -1)])
			])])]), Y("details", aI, [n[52] ||= Y("summary", null, "Technical endpoints", -1), Y("div", null, [
				n[49] ||= Y("span", null, "LLM route", -1),
				Y("code", null, V(e.llmApiUrl), 1),
				n[50] ||= Y("span", null, "Model gateway", -1),
				Y("code", null, V(e.modelsApiUrl), 1),
				n[51] ||= Y("span", null, "Pair", -1),
				Y("code", null, V(e.pairApiUrl), 1)
			])])], 64)),
			s.value === "pair" ? Z("", !0) : (q(), J("footer", oI, [Y("div", null, [Y("strong", null, V(d.value ? "Unsaved Spud Link changes" : "Spud Link is synchronized"), 1), n[53] ||= Y("span", null, "Live connection status continues to refresh while this tab is open.", -1)]), Y("button", {
				class: "tv-button primary",
				type: "button",
				disabled: c.value || !d.value,
				onClick: P
			}, V(c.value ? "Saving…" : "Save Spud Link settings"), 9, sI)])),
			(q(), da(Un, { to: "body" }, [_.open ? (q(), J("div", {
				key: 0,
				class: "tlink-modal-backdrop",
				role: "presentation",
				onClick: bs(te, ["self"])
			}, [Y("section", cI, [Y("header", null, [Y("div", null, [n[54] ||= Y("span", { class: "tv-eyebrow" }, "Spud Link", -1), Y("h2", lI, V(_.role === "spudlet" ? "Link a Spudlet" : "Pair a Little Spud"), 1)]), Y("button", {
				class: "tv-button",
				type: "button",
				onClick: te
			}, "Close")]), _.phase === "loading" ? (q(), J("div", uI, [
				n[55] ||= Y("span", { class: "tlink-loader" }, null, -1),
				n[56] ||= Y("strong", null, "Creating a secure invitation…", -1),
				Y("small", null, V(_.message), 1)
			])) : _.phase === "success" ? (q(), J("div", dI, [
				n[57] ||= Y("span", { class: "tlink-check" }, "✓", -1),
				n[58] ||= Y("strong", null, "Connected!", -1),
				Y("small", null, V(_.message), 1)
			])) : _.phase === "error" ? (q(), J("div", fI, [
				n[59] ||= Y("span", { class: "tlink-check" }, "!", -1),
				n[60] ||= Y("strong", null, "Pairing failed", -1),
				Y("small", null, V(_.message), 1)
			])) : (q(), J("div", pI, [
				_.role === "little_spud" ? (q(), J("img", {
					key: 0,
					class: "tlink-qr",
					src: _.qr,
					alt: "Little Spud pairing QR code"
				}, null, 8, mI)) : (q(), J("div", hI, [Y("code", null, V(_.code), 1), Y("button", {
					class: "tv-button",
					type: "button",
					onClick: R
				}, "Copy")])),
				Y("strong", null, V(_.message), 1),
				Y("small", null, [n[61] ||= X("Waiting for connection", -1), _.expiresAt ? (q(), J("span", gI, " · expires " + V((/* @__PURE__ */ new Date(_.expiresAt * 1e3)).toLocaleTimeString([], {
					hour: "numeric",
					minute: "2-digit"
				})), 1)) : Z("", !0)])
			]))])])) : Z("", !0)]))
		]));
	}
}), vI = { class: "system-task-process" }, yI = { key: 0 }, bI = { class: "system-task-cell" }, xI = { class: "system-task-cell" }, SI = { class: "system-task-cell" }, CI = {
	key: 0,
	class: "system-task-error"
}, wI = { class: "system-task-command" }, TI = ["disabled"], EI = {
	key: 1,
	class: "tv-button",
	type: "button",
	disabled: ""
}, DI = /* @__PURE__ */ sr({
	__name: "SystemTaskCard",
	props: {
		task: {},
		coreKey: { default: "" },
		busy: {
			type: Boolean,
			default: !1
		}
	},
	emits: ["run"],
	setup(e, { emit: t }) {
		let n = e, r = t;
		function i(e, t = !1) {
			if (typeof e == "boolean") return e;
			if (typeof e == "number") return e !== 0;
			if (typeof e == "string") {
				let t = e.trim().toLowerCase();
				if ([
					"true",
					"1",
					"yes",
					"on",
					"enabled"
				].includes(t)) return !0;
				if ([
					"false",
					"0",
					"no",
					"off",
					"disabled"
				].includes(t)) return !1;
			}
			return t;
		}
		let a = Q(() => String(n.task.id || "").trim()), o = Q(() => String(n.task.label || a.value || "System task").trim()), s = Q(() => String(n.task.description || "").trim()), c = Q(() => String(n.task.status || "idle").trim().toLowerCase()), l = Q(() => i(n.task.running)), u = Q(() => i(n.task.enabled, !0)), d = Q(() => !n.coreKey || i(n.task.manual, !0)), f = Q(() => n.coreKey ? i(n.task.can_run) : u.value), p = Q(() => n.busy || l.value || !f.value || !d.value), m = Q(() => String(n.task.schedule_label || "").trim() || b(n.task.interval_seconds)), h = Q(() => n.busy ? "Starting" : l.value ? "Running" : c.value === "error" ? "Needs attention" : c.value === "stopped" ? "Core stopped" : c.value === "waiting" ? "Waiting" : u.value ? "Ready" : "Disabled"), g = Q(() => n.busy || l.value ? "running" : u.value ? c.value || "idle" : "disabled"), _ = Q(() => {
			let e = Math.max(0, Number(n.task.duration_ms || 0));
			return e ? e < 1e3 ? `${e.toFixed(0)} ms` : `${(e / 1e3).toFixed(1)} s` : "—";
		}), v = Q(() => String(n.task.next_run_label || "").trim() || (l.value ? "After this run" : u.value ? y(n.task.next_run_at, !0) : "Off"));
		function y(e, t = !1) {
			let n = Number(e || 0);
			if (!Number.isFinite(n) || n <= 0) return t ? "Not scheduled" : "Not run yet";
			let r = Math.round(n - Date.now() / 1e3);
			if (t && r > 0 && r < 90) return `in ${r}s`;
			try {
				return (/* @__PURE__ */ new Date(n * 1e3)).toLocaleString([], {
					month: "short",
					day: "numeric",
					hour: "numeric",
					minute: "2-digit",
					second: "2-digit"
				});
			} catch {
				return t ? "Scheduled" : "Completed";
			}
		}
		function b(e) {
			let t = Math.max(0, Number(e || 0));
			return t ? t < 60 ? `Every ${Math.round(t)}s` : t < 3600 ? `Every ${Math.round(t / 60)}m` : t < 86400 ? `Every ${Math.round(t / 3600)}h` : `Every ${Math.round(t / 86400)}d` : "Off";
		}
		return (t, n) => (q(), J("article", { class: B(["system-task-card", `tone-${g.value}`]) }, [
			Y("div", vI, [
				Y("div", null, [n[1] ||= Y("i", { class: "system-task-process-dot" }, null, -1), Y("h3", null, V(o.value), 1)]),
				s.value ? (q(), J("p", yI, V(s.value), 1)) : Z("", !0),
				Y("small", null, V(Number(e.task.run_count || 0) ? `${Number(e.task.run_count)} completed run${Number(e.task.run_count) === 1 ? "" : "s"}` : "Waiting for its first run"), 1)
			]),
			Y("div", bI, [n[2] ||= Y("span", null, "Schedule", -1), Y("strong", null, V(m.value), 1)]),
			Y("div", xI, [
				n[3] ||= Y("span", null, "Last activity", -1),
				Y("strong", null, V(y(e.task.finished_at)), 1),
				Y("small", null, V(_.value === "—" ? "No duration yet" : _.value), 1)
			]),
			Y("div", SI, [Y("span", null, V(e.task.next_run_label ? "Trigger" : "Next / trigger"), 1), Y("strong", null, V(v.value), 1)]),
			e.task.last_error ? (q(), J("div", CI, V(e.task.last_error), 1)) : Z("", !0),
			Y("div", wI, [Y("span", { class: B(["system-task-status", `tone-${g.value}`]) }, [n[4] ||= Y("i", null, null, -1), X(V(h.value), 1)], 2), d.value ? (q(), J("button", {
				key: 0,
				class: "tv-button",
				type: "button",
				disabled: p.value,
				onClick: n[0] ||= (t) => r("run", a.value, e.coreKey)
			}, V(e.busy ? "Starting…" : l.value ? "Running…" : "Run now"), 9, TI)) : (q(), J("button", EI, "Automatic"))])
		], 2));
	}
}), OI = { class: "tset-resource tsystem-tasks" }, kI = {
	key: 0,
	class: "tv-notice error",
	"aria-live": "polite"
}, AI = { class: "tv-panel tset-form-card" }, jI = { class: "tsystem-head" }, MI = { class: "tsystem-live-state" }, NI = { class: "system-task-health" }, PI = { key: 0 }, FI = { class: "system-task-summary" }, II = {
	key: 1,
	class: "system-task-sections"
}, LI = { class: "system-task-section" }, RI = { class: "system-task-section-head" }, zI = {
	key: 0,
	class: "system-task-grid"
}, BI = {
	key: 1,
	class: "tv-empty"
}, VI = { class: "system-task-section" }, HI = { class: "system-task-section-head" }, UI = { class: "system-task-core-groups" }, WI = { class: "system-task-group-head" }, GI = {
	key: 0,
	class: "system-task-grid"
}, KI = {
	key: 1,
	class: "tv-empty"
}, qI = {
	key: 0,
	class: "tv-empty"
}, JI = /* @__PURE__ */ sr({
	__name: "SystemTasksSettings",
	props: {
		endpoint: {},
		coreRunEndpoint: {}
	},
	emits: ["notify"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ U({}), a = /* @__PURE__ */ U(!1), o = /* @__PURE__ */ U(!1), s = /* @__PURE__ */ U(""), c = /* @__PURE__ */ U(0), l = /* @__PURE__ */ U([]), u = !1, d = 0, f = Q(() => Array.isArray(i.value.tasks) ? i.value.tasks : []), p = Q(() => Array.isArray(i.value.core_tasks) ? i.value.core_tasks : []), m = Q(() => Array.isArray(i.value.core_task_errors) ? i.value.core_task_errors : []), h = Q(() => p.value.flatMap((e) => Array.isArray(e.tasks) ? e.tasks : [])), g = Q(() => [...f.value, ...h.value]), _ = Q(() => g.value.length), v = Q(() => g.value.filter((e) => k(e.running)).length), y = Q(() => g.value.filter((e) => String(e.status || "").toLowerCase() === "error").length + m.value.length), b = Q(() => g.value.filter((e) => !k(e.enabled, !0)).length), x = Q(() => g.value.filter((e) => k(e.enabled, !0) && !k(e.running) && String(e.status || "").toLowerCase() !== "error").length), S = Q(() => Math.max(0, _.value - Math.min(_.value, y.value))), C = Q(() => _.value ? Math.round(S.value / _.value * 100) : 0), w = Q(() => ({ "--system-health-angle": `${C.value * 3.6}deg` })), T = Q(() => y.value ? "attention" : v.value ? "running" : "ready"), E = Q(() => y.value ? "Needs attention" : v.value ? "Work in progress" : _.value ? "All processes ready" : "Waiting for task data"), D = Q(() => s.value ? "Update issue" : o.value ? "Live" : "Connecting"), O = Q(() => c.value ? new Date(c.value).toLocaleTimeString([], {
			hour: "numeric",
			minute: "2-digit",
			second: "2-digit"
		}) : "Not loaded");
		function k(e, t = !1) {
			if (typeof e == "boolean") return e;
			if (typeof e == "number") return e !== 0;
			if (typeof e == "string") {
				let t = e.trim().toLowerCase();
				if ([
					"true",
					"1",
					"yes",
					"on",
					"enabled"
				].includes(t)) return !0;
				if ([
					"false",
					"0",
					"no",
					"off",
					"disabled"
				].includes(t)) return !1;
			}
			return t;
		}
		function A(e, t = "") {
			return `${t || "tater"}:${e}`;
		}
		function j(e, t = "") {
			return l.value.includes(A(String(e || ""), t));
		}
		function M(e, t) {
			let n = new Set(l.value);
			t ? n.add(e) : n.delete(e), l.value = [...n];
		}
		function N() {
			d && window.clearTimeout(d), d = 0;
		}
		function P(e = 2e3) {
			N(), u && (d = window.setTimeout(async () => {
				d = 0, await F(!0), P(2e3);
			}, Math.max(500, e)));
		}
		async function F(e = !1) {
			if (!a.value) {
				a.value = !0, e || (s.value = "");
				try {
					i.value = await As(n.endpoint), o.value = !0, s.value = "", c.value = Date.now();
				} catch (e) {
					s.value = e instanceof Error ? e.message : "System tasks could not be loaded.";
				} finally {
					a.value = !1;
				}
			}
		}
		async function I(e, t = "") {
			let a = A(e, t);
			if (!e || l.value.includes(a)) return;
			M(a, !0), s.value = "", N();
			let u = t ? `${n.coreRunEndpoint}/${encodeURIComponent(t)}/${encodeURIComponent(e)}/run` : `${n.endpoint}/${encodeURIComponent(e)}/run`;
			try {
				let e = await js(u);
				i.value = e, o.value = !0, c.value = Date.now(), r("notify", e.queued === !1 ? "That task is already running." : "System task started.", "success");
			} catch (e) {
				s.value = e instanceof Error ? e.message : "System task could not be started.", r("notify", s.value, "error");
			} finally {
				M(a, !1), P(500);
			}
		}
		return Er(async () => {
			u = !0, await F(), P(2e3);
		}), kr(() => {
			u = !1, N();
		}), (e, t) => (q(), J("section", OI, [
			s.value ? (q(), J("div", kI, V(s.value), 1)) : Z("", !0),
			Y("section", AI, [Y("header", jI, [t[1] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Background work"),
				Y("h2", null, "System tasks"),
				Y("p", null, "Live status for snapshots, maintenance jobs, and tasks provided by installed Cores.")
			], -1), Y("div", {
				class: B(["tsystem-live", { issue: !!s.value }]),
				role: "status",
				"aria-live": "polite"
			}, [Y("span", MI, [t[0] ||= Y("i", null, null, -1), X(V(D.value), 1)]), Y("small", null, V(o.value ? `Updated ${O.value}` : "Loading process status…"), 1)], 2)]), Y("div", {
				class: B(["system-task-overview", `tone-${T.value}`]),
				"aria-live": "polite"
			}, [Y("div", NI, [Y("div", {
				class: "system-task-health-ring",
				style: R(w.value)
			}, [Y("span", null, V(C.value) + "%", 1), t[2] ||= Y("small", null, "healthy", -1)], 4), Y("div", null, [
				t[3] ||= Y("span", null, "Overall process health", -1),
				Y("strong", null, V(E.value), 1),
				Y("small", null, [X(V(f.value.length) + " Tater · " + V(h.value.length) + " Core", 1), b.value ? (q(), J("span", PI, " · " + V(b.value) + " disabled", 1)) : Z("", !0)])
			])]), Y("div", FI, [
				Y("div", null, [t[4] ||= Y("span", null, "Processes", -1), Y("strong", null, V(_.value), 1)]),
				Y("div", null, [t[5] ||= Y("span", null, "Running", -1), Y("strong", null, V(v.value), 1)]),
				Y("div", null, [t[6] ||= Y("span", null, "Ready", -1), Y("strong", null, V(x.value), 1)]),
				Y("div", { class: B({ alert: y.value }) }, [t[7] ||= Y("span", null, "Issues", -1), Y("strong", null, V(y.value), 1)], 2)
			])], 2)]),
			o.value ? (q(), J("div", II, [Y("section", LI, [Y("header", RI, [t[8] ||= Y("div", null, [Y("span", null, "Tater"), Y("h2", null, "Scheduled tasks")], -1), Y("strong", null, V(f.value.length) + " process" + V(f.value.length === 1 ? "" : "es"), 1)]), f.value.length ? (q(), J("div", zI, [t[9] ||= Y("div", {
				class: "system-task-list-head",
				"aria-hidden": "true"
			}, [
				Y("span", null, "Process"),
				Y("span", null, "Schedule"),
				Y("span", null, "Last activity"),
				Y("span", null, "Next / trigger"),
				Y("span", null, "Status")
			], -1), (q(!0), J(K, null, G(f.value, (e) => (q(), da(DI, {
				key: e.id,
				task: e,
				busy: j(e.id),
				onRun: I
			}, null, 8, ["task", "busy"]))), 128))])) : (q(), J("p", BI, "No Tater system tasks are registered."))]), Y("section", VI, [Y("header", HI, [t[10] ||= Y("div", null, [Y("span", null, "Installed Cores"), Y("h2", null, "Core tasks")], -1), Y("strong", null, V(h.value.length) + " process" + V(h.value.length === 1 ? "" : "es"), 1)]), Y("div", UI, [
				(q(!0), J(K, null, G(p.value, (e) => (q(), J("section", {
					key: e.core_key,
					class: "system-task-core-group"
				}, [Y("header", WI, [Y("div", null, [t[11] ||= Y("span", null, "Core tasks", -1), Y("h3", null, V(e.label || e.core_key || "Core"), 1)]), Y("span", { class: B(["system-task-core-state", e.running ? "running" : "stopped"]) }, [t[12] ||= Y("i", null, null, -1), X(V(e.running ? "Core running" : "Core stopped"), 1)], 2)]), Array.isArray(e.tasks) && e.tasks.length ? (q(), J("div", GI, [t[13] ||= Y("div", {
					class: "system-task-list-head",
					"aria-hidden": "true"
				}, [
					Y("span", null, "Process"),
					Y("span", null, "Schedule"),
					Y("span", null, "Last activity"),
					Y("span", null, "Next / trigger"),
					Y("span", null, "Status")
				], -1), (q(!0), J(K, null, G(e.tasks, (t) => (q(), da(DI, {
					key: t.id,
					task: t,
					"core-key": String(e.core_key || ""),
					busy: j(t.id, String(e.core_key || "")),
					onRun: I
				}, null, 8, [
					"task",
					"core-key",
					"busy"
				]))), 128))])) : (q(), J("p", KI, "This Core does not expose background tasks."))]))), 128)),
				p.value.length ? Z("", !0) : (q(), J("p", qI, "No installed Cores expose background tasks yet.")),
				(q(!0), J(K, null, G(m.value, (e) => (q(), J("div", {
					key: `${e.core_key}-${e.error}`,
					class: "tv-notice error"
				}, V(e.core_key || "Core") + ": " + V(e.error || "Task status unavailable."), 1))), 128))
			])])])) : Z("", !0)
		]));
	}
});
//#endregion
//#region src/settings/components/voice/runtime.ts
function YI(e, t) {
	let n = e.ui && typeof e.ui == "object" ? e.ui : {}, r = Array.isArray(n.item_forms) ? n.item_forms : [];
	return t ? r.filter((e) => String(e.group || "") === t) : r;
}
function XI(e = []) {
	let t = {};
	return e.forEach((e) => {
		let n = String(e.key || e.id || "").trim(), r = String(e.type || "").trim().toLowerCase();
		!n || e.disabled || e.read_only || e.readonly || [
			"table",
			"readonly",
			"section",
			"led_preview"
		].includes(r) || (t[n] = e.value ?? e.default ?? (String(e.type || "") !== "checkbox" && ""));
	}), t;
}
function ZI(e = []) {
	let t = {};
	return e.forEach((e) => Object.assign(t, XI(Array.isArray(e.fields) ? e.fields : []))), t;
}
function QI(e) {
	return e && Array.isArray(e.sections) ? e.sections : [];
}
async function $I(e, t, n = {}) {
	return js(e, {
		action: t,
		payload: n
	});
}
function eL(e) {
	if (e && typeof e == "object") {
		let t = e;
		return String(t.value ?? t.id ?? t.key ?? "");
	}
	return String(e ?? "");
}
function tL(e) {
	if (e && typeof e == "object") {
		let t = e;
		return String(t.label ?? t.name ?? t.value ?? t.id ?? "");
	}
	return String(e ?? "");
}
//#endregion
//#region src/settings/components/voice/VoiceFirmware.vue?vue&type=script&setup=true&lang.ts
var nL = { class: "tm-stack tvoice-firmware" }, rL = {
	key: 0,
	class: "tv-notice error"
}, iL = { class: "tm-form-card tvoice-firmware-overview" }, aL = { class: "tvoice-firmware-summary" }, oL = {
	key: 0,
	class: "tvoice-updates"
}, sL = ["disabled"], cL = { class: "tvoice-update-list" }, lL = ["disabled", "onClick"], uL = {
	key: 1,
	class: "tm-status-card live"
}, dL = { class: "tm-inline-actions tvoice-firmware-primary-actions" }, fL = ["disabled"], pL = ["disabled"], mL = { class: "tvoice-progress-icon" }, hL = {
	class: "tv-modal tvoice-flasher-modal",
	role: "dialog",
	"aria-modal": "true",
	"aria-labelledby": "tvoice-flasher-title"
}, gL = {
	class: "tvoice-flash-methods",
	role: "tablist",
	"aria-label": "Firmware install method"
}, _L = { class: "tvoice-flash-step" }, vL = { class: "tvoice-target-picker" }, yL = ["onClick"], bL = ["src", "alt"], xL = {
	key: 0,
	class: "tv-notice"
}, SL = {
	key: 0,
	class: "tvoice-flash-step"
}, CL = { class: "tvoice-image-picker" }, wL = {
	key: 1,
	class: "tvoice-flash-step"
}, TL = { class: "tm-field" }, EL = ["value"], DL = {
	key: 2,
	class: "tvoice-firmware-target"
}, OL = ["src", "alt"], kL = { class: "tm-detail-list" }, AL = {
	key: 3,
	class: "tm-status-card live"
}, jL = ["href"], ML = { class: "tvoice-flasher-footer" }, NL = ["disabled"], PL = ["disabled"], FL = ["disabled"], IL = ["disabled"], LL = ["disabled"], RL = {
	class: "tv-modal tvoice-progress-modal",
	role: "dialog",
	"aria-modal": "true",
	"aria-labelledby": "tvoice-progress-title"
}, zL = { class: "tv-eyebrow" }, BL = { id: "tvoice-progress-title" }, VL = ["value"], HL = {
	key: 0,
	class: "tvoice-update-queue"
}, UL = { class: "tvoice-firmware-session" }, WL = ["disabled"], GL = /* @__PURE__ */ sr({
	__name: "VoiceFirmware",
	props: {
		payload: {},
		actionEndpoint: {}
	},
	emits: ["refresh", "notify"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ U("ota"), a = /* @__PURE__ */ U(""), o = /* @__PURE__ */ U(""), s = /* @__PURE__ */ U("factory"), c = /* @__PURE__ */ U([]), l = /* @__PURE__ */ U(""), u = /* @__PURE__ */ U(""), d = /* @__PURE__ */ U(""), f = /* @__PURE__ */ U(null), p = /* @__PURE__ */ U([]), m = /* @__PURE__ */ U(null), h = /* @__PURE__ */ U(0), g = /* @__PURE__ */ U(!1), _ = /* @__PURE__ */ U(!1), v = /* @__PURE__ */ U("Firmware update"), y = /* @__PURE__ */ U(""), b = /* @__PURE__ */ U([]), x = /* @__PURE__ */ U(-1), S = /* @__PURE__ */ U(!1), C = null, w = Q(() => n.payload.firmware && typeof n.payload.firmware == "object" ? n.payload.firmware : {}), T = Q(() => Array.isArray(w.value.templates) ? w.value.templates : []), E = Q(() => Array.isArray(w.value.devices) ? w.value.devices : []), D = Q(() => E.value.filter((e) => !!e.connected && String(e.value || "") !== "__usb_recovery__")), O = Q(() => Array.isArray(w.value.firmware_updates) ? w.value.firmware_updates : []), k = Q(() => Array.isArray(w.value.firmware_flash_targets) ? w.value.firmware_flash_targets : []), A = Q(() => Array.isArray(w.value.warnings) ? w.value.warnings.map(String) : []), j = Q(() => w.value.prebuilt_firmware && typeof w.value.prebuilt_firmware == "object" ? w.value.prebuilt_firmware : {}), M = Q(() => w.value.variants && typeof w.value.variants == "object" ? w.value.variants : {}), N = Q(() => T.value.filter((e) => {
			let t = M.value[eL(e)];
			return !!(t && typeof t == "object" && t.__usb_recovery__);
		})), P = Q(() => i.value === "ota" ? D.value : N.value), F = Q(() => {
			let e = a.value, t = i.value === "ota" ? o.value : "__usb_recovery__", n = M.value[e];
			return n && typeof n == "object" && n[t] && typeof n[t] == "object" ? n[t] : {};
		}), I = Q(() => F.value.prebuilt_firmware && typeof F.value.prebuilt_firmware == "object" ? F.value.prebuilt_firmware : {}), ee = Q(() => I.value.artifacts && typeof I.value.artifacts == "object" ? I.value.artifacts : {}), te = Q(() => ee.value.factory && typeof ee.value.factory == "object" ? ee.value.factory : {}), ne = Q(() => ee.value.ota && typeof ee.value.ota == "object" ? ee.value.ota : {}), L = Q(() => s.value === "ota" ? ne.value : te.value), R = Q(() => String(te.value.flash_transport || "esp_serial").toLowerCase()), z = Q(() => !!(o.value && a.value && ne.value.path && (F.value.connected ?? !0))), re = Q(() => !!(a.value && L.value.path)), ie = Q(() => {
			let e = String(f.value?.phase || f.value?.status || "").toLowerCase();
			return ![
				"completed",
				"failed",
				"cancelled",
				"stopped"
			].includes(e) && !!(u.value || f.value?.session_id);
		}), ae = Q(() => Math.max(0, Math.min(100, Number(f.value?.progress_percent ?? f.value?.progress ?? 0)))), oe = Q(() => b.value.filter((e) => String(e.status) === "completed").length), se = Q(() => b.value.length ? Math.max(0, Math.min(100, (oe.value + (ie.value ? ae.value / 100 : 0)) / b.value.length * 100)) : ae.value), ce = Q(() => String(f.value?.phase || f.value?.status || "starting").toLowerCase()), le = Q(() => b.value.length ? `${oe.value} of ${b.value.length} complete` : `${Math.round(ae.value)}% complete`), ue = Q(() => x.value >= 0 && b.value[x.value] || null);
		function de(e) {
			let t = E.value.find((t) => String(t.value || "") === e);
			return String(t?.template_key || "");
		}
		function fe() {
			if (i.value === "ota") {
				let e = D.value.map((e) => String(e.value || ""));
				e.includes(o.value) || (o.value = String(w.value.active_selector || e[0] || "")), a.value = de(o.value) || String(w.value.active_template_key || "");
			} else {
				let e = N.value.map(eL);
				e.includes(a.value) || (a.value = String(w.value.active_template_key || e[0] || "")), o.value = "__usb_recovery__";
			}
		}
		function pe(e) {
			i.value = e, m.value = null, c.value = [], l.value = "", fe(), e === "local_usb" && je(!1);
		}
		function me(e) {
			i.value === "ota" ? (o.value = e, a.value = de(e)) : a.value = e, m.value = null, c.value = [], l.value = "", i.value === "local_usb" && je(!1);
		}
		function he(e, t = "") {
			let n = E.value.find((t) => String(t.value || t.selector || "") === e), r = T.value.find((e) => eL(e) === t);
			return tL(n || r || { label: e || t || "Satellite" });
		}
		function ge() {
			fe(), g.value = !0, i.value === "local_usb" && je(!1);
		}
		function _e() {
			_.value = !1;
		}
		function ve() {
			_.value = !0;
		}
		async function ye(e, t = {}) {
			return $I(n.actionEndpoint, e, t);
		}
		function be(e = o.value, t = a.value) {
			return {
				id: e,
				selector: e,
				template_key: t
			};
		}
		function xe(e) {
			let t = String(e || "").trim();
			return !t || /^(?:https?:)?\/\//i.test(t) ? t : `${n.actionEndpoint.endsWith("/api/settings/voice/runtime/action") ? n.actionEndpoint.slice(0, -34) : ""}${t.startsWith("/") ? t : `/${t}`}`;
		}
		function H(e) {
			let t = Array.isArray(e.entries) ? e.entries : [], n = String(e.session_id || f.value?.session_id || "session"), r = new Set(p.value.map((e) => `${e._session_id || "session"}:${e.seq ?? `${e.time}:${e.message || e.display}`}`));
			t.forEach((e) => {
				let t = `${n}:${e.seq ?? `${e.time}:${e.message || e.display}`}`;
				r.has(t) || (r.add(t), p.value.push({
					...e,
					_session_id: n
				}));
			}), p.value.length > 500 && (p.value = p.value.slice(-500));
		}
		function Se(e) {
			f.value = {
				...f.value || {},
				...e
			}, H(e), ue.value && (ue.value.progress = ae.value, ue.value.message = e.message || e.phase || e.status || "Updating");
		}
		function Ce(e) {
			return p.value.filter((t) => String(t._session_id || "session") === e).reduce((e, t) => Math.max(e, Number(t.seq || 0)), 0);
		}
		async function we() {
			let e = String(f.value?.session_id || "");
			if (e) try {
				let t = await ye("voice_firmware_flash_poll", {
					session_id: e,
					after_seq: Ce(e)
				});
				h.value = 0, d.value = "", Se(t), ie.value ? Te() : (u.value = "", r("notify", String(t.message || (String(t.phase) === "completed" ? "Firmware operation completed." : "Firmware operation stopped.")), String(t.phase) === "failed" ? "error" : "success"), r("refresh"));
			} catch (e) {
				if (h.value += 1, ie.value && h.value <= 120) {
					let e = !!f.value?.self_ota_recovery;
					d.value = e ? "Tater Embedded is restarting. Reconnecting to verify the update…" : "Firmware progress is temporarily unavailable. Reconnecting…", Te();
					return;
				}
				d.value = e instanceof Error ? e.message : "Firmware progress could not be refreshed.", f.value = {
					...f.value || {},
					phase: "failed",
					message: d.value
				}, u.value = "";
			}
		}
		function Te() {
			Ee(), C = window.setTimeout(we, 1100);
		}
		function Ee() {
			C !== null && window.clearTimeout(C), C = null;
		}
		async function De(e, t, n, i = "Firmware update", a = "") {
			if (!u.value) {
				u.value = n, d.value = "", f.value = null, p.value = [], b.value = [], x.value = -1, S.value = !1, v.value = i, y.value = a, g.value = !1, _.value = !0, h.value = 0;
				try {
					let n = await ye(e, t);
					Se(n), r("notify", String(n.message || "Firmware operation started."), "success"), n.session_id && ie.value ? Te() : u.value = "";
				} catch (e) {
					d.value = e instanceof Error ? e.message : "Firmware operation could not start.", f.value = {
						phase: "failed",
						progress_percent: 0,
						message: d.value
					}, u.value = "", r("notify", d.value, "error");
				}
			}
		}
		async function Oe(e) {
			let t = String(e?.selector || o.value), n = String(e?.template_key || a.value);
			await De("voice_firmware_flash_start", {
				...be(t, n),
				follow_logs: !0
			}, "ota", "Installing firmware", String(e?.title || he(t, n)));
		}
		async function ke(e = O.value.length ? O.value : k.value) {
			if (!(!e.length || u.value)) {
				u.value = "batch", d.value = "", f.value = null, p.value = [], S.value = !1, b.value = e.map((e, t) => ({
					key: `${e.selector || "satellite"}:${e.template_key || t}`,
					title: e.title || he(String(e.selector || ""), String(e.template_key || "")),
					subtitle: `${e.installed || "unknown"} → ${e.latest || "latest"}`,
					status: "queued",
					progress: 0
				})), x.value = 0, v.value = e.length === 1 ? "Installing firmware" : "Updating satellites", y.value = `${e.length} satellite${e.length === 1 ? "" : "s"}`, _.value = !0;
				for (let t = 0; t < e.length && !S.value; t += 1) {
					let n = e[t];
					x.value = t, b.value[t].status = "running", p.value.push({
						level: "info",
						display: `Starting ${t + 1} of ${e.length}: ${n.title || n.selector}`
					});
					try {
						let e = await ye("voice_firmware_flash_start", {
							...be(String(n.selector || ""), String(n.template_key || "")),
							follow_logs: !0
						});
						for (Se(e); !S.value && e.session_id && ![
							"completed",
							"failed",
							"cancelled",
							"stopped"
						].includes(String(e.phase || e.status || "").toLowerCase());) {
							await new Promise((e) => window.setTimeout(e, 1100));
							let t = Ce(String(e.session_id || ""));
							e = await ye("voice_firmware_flash_poll", {
								session_id: e.session_id,
								after_seq: t
							}), Se(e);
						}
						if (S.value) {
							b.value[t].status = "stopped";
							break;
						}
						if (String(e.phase || "").toLowerCase() === "failed") throw Error(String(e.error || e.message || "Firmware update failed."));
						b.value[t].status = "completed", b.value[t].progress = 100;
					} catch (e) {
						b.value[t].status = "failed", b.value[t].message = e instanceof Error ? e.message : "Firmware update failed.", d.value = e instanceof Error ? e.message : "Firmware batch stopped.", r("notify", d.value, "error"), u.value = "";
						return;
					}
				}
				if (u.value = "", x.value = -1, S.value) {
					r("notify", "Firmware update queue stopped.", "error");
					return;
				}
				f.value = {
					...f.value || {},
					phase: "completed",
					progress_percent: 100,
					message: `Updated ${e.length} satellite${e.length === 1 ? "" : "s"}.`
				}, r("notify", `Updated ${e.length} satellite${e.length === 1 ? "" : "s"}.`, "success"), r("refresh");
			}
		}
		async function Ae() {
			let e = String(f.value?.session_id || "");
			if (Ee(), S.value = !0, !e) {
				f.value = {
					...f.value || {},
					phase: "stopped",
					message: "Firmware operation stopped."
				}, u.value = "";
				return;
			}
			try {
				Se(await ye("voice_firmware_flash_stop", { session_id: e }));
			} catch (e) {
				d.value = e instanceof Error ? e.message : "Firmware session could not be stopped.";
			}
			u.value = "";
		}
		async function je(e = !0) {
			if (!(!a.value || R.value === "amlogic_usb_burn")) try {
				let t = await ye("voice_firmware_esp_usb_ports", be("__usb_recovery__", a.value));
				c.value = Array.isArray(t.ports) ? t.ports : [], c.value.some((e) => eL(e) === l.value) || (l.value = eL(c.value[0] || "")), e && r("notify", String(t.message || `Found ${c.value.length} serial port(s).`), t.available === !1 ? "error" : "success");
			} catch (e) {
				d.value = e instanceof Error ? e.message : "USB ports could not be loaded.";
			}
		}
		async function Me() {
			if (R.value === "amlogic_usb_burn") {
				await De("voice_firmware_amlogic_flash_start", {
					...be("__usb_recovery__"),
					flash_kind: "factory"
				}, "local-usb", "USB recovery", he("", a.value));
				return;
			}
			!l.value && (await je(), !l.value) || await De("voice_firmware_esp_usb_flash_start", {
				...be("__usb_recovery__"),
				serial_port: l.value,
				flash_kind: s.value
			}, "local-usb", "USB firmware flash", he("", a.value));
		}
		async function Ne() {
			if (!l.value) try {
				let e = await ye("voice_firmware_local_usb_log_ports", { template_key: a.value });
				c.value = Array.isArray(e.ports) ? e.ports : [], l.value = eL(c.value[0] || "");
			} catch (e) {
				d.value = e instanceof Error ? e.message : "USB log ports could not be loaded.";
				return;
			}
			if (!l.value) {
				d.value = "Connect a USB serial device first.";
				return;
			}
			await De("voice_firmware_local_usb_log_start", {
				...be("__usb_recovery__"),
				serial_port: l.value
			}, "usb-logs", "Live USB logs", he("", a.value));
		}
		async function Pe() {
			if (!(u.value || !re.value)) {
				u.value = "browser", d.value = "";
				try {
					m.value = await ye("voice_firmware_browser_build", {
						...be("__usb_recovery__"),
						flash_kind: s.value
					}), r("notify", String(m.value.message || "Browser USB image prepared."), "success");
				} catch (e) {
					d.value = e instanceof Error ? e.message : "Browser USB image could not be prepared.", r("notify", d.value, "error");
				} finally {
					u.value = "";
				}
			}
		}
		async function Fe() {
			if (window.confirm("Clean downloaded firmware files and completed firmware sessions?")) {
				u.value = "clean";
				try {
					let e = await ye("voice_firmware_clean");
					r("notify", String(e.message || "Firmware cache cleaned."), "success"), r("refresh");
				} catch (e) {
					d.value = e instanceof Error ? e.message : "Firmware cache could not be cleaned.";
				} finally {
					u.value = "";
				}
			}
		}
		return On(w, fe, {
			deep: !0,
			immediate: !0
		}), On(o, (e) => {
			i.value === "ota" && (a.value = de(e));
		}), On(ne, (e) => {
			e.path || (s.value = "factory");
		}, {
			deep: !0,
			immediate: !0
		}), kr(Ee), (e, t) => (q(), J("section", nL, [
			d.value ? (q(), J("div", rL, V(d.value), 1)) : Z("", !0),
			(q(!0), J(K, null, G(A.value, (e) => (q(), J("div", {
				key: e,
				class: "tv-notice warning"
			}, V(e), 1))), 128)),
			Y("article", iL, [
				Y("header", null, [Y("div", null, [
					t[12] ||= Y("span", { class: "tv-eyebrow" }, "Official Tater firmware", -1),
					Y("h3", null, V(j.value.available ? j.value.version || "Latest release ready" : "Firmware manifest unavailable"), 1),
					t[13] ||= Y("p", null, "Tater matches each connected satellite to its verified firmware family and installs updates one device at a time.", -1)
				]), Y("span", { class: B(["tv-live-pill", { warning: !j.value.available }]) }, [t[14] ||= Y("i", null, null, -1), X(V(j.value.available ? `${j.value.device_count || 0} families ready` : "Unavailable"), 1)], 2)]),
				Y("div", aL, [
					Y("div", null, [t[15] ||= Y("span", null, "Connected targets", -1), Y("strong", null, V(k.value.length), 1)]),
					Y("div", { class: B({ attention: O.value.length }) }, [t[16] ||= Y("span", null, "Updates ready", -1), Y("strong", null, V(O.value.length), 1)], 2),
					t[17] ||= Y("div", null, [Y("span", null, "Install method"), Y("strong", null, "Verified OTA")], -1)
				]),
				O.value.length ? (q(), J("section", oL, [Y("header", null, [t[18] ||= Y("div", null, [Y("h4", null, "Ready to update"), Y("p", null, "Current and available versions are shown for every satellite.")], -1), Y("button", {
					class: "tv-button primary",
					type: "button",
					disabled: !!u.value,
					onClick: t[0] ||= (e) => ke(O.value)
				}, "Update All (" + V(O.value.length) + ")", 9, sL)]), Y("div", cL, [(q(!0), J(K, null, G(O.value, (e) => (q(), J("div", { key: `${e.selector}:${e.template_key}` }, [
					t[20] ||= Y("span", { class: "tvoice-update-mark" }, "↑", -1),
					Y("div", null, [Y("strong", null, V(e.title || e.selector), 1), Y("span", null, [
						Y("b", null, V(e.installed || "unknown"), 1),
						t[19] ||= Y("i", null, "→", -1),
						Y("b", null, V(e.latest || "latest"), 1)
					])]),
					Y("button", {
						class: "tv-button",
						type: "button",
						disabled: !!u.value,
						onClick: (t) => Oe(e)
					}, "Update", 8, lL)
				]))), 128))])])) : (q(), J("div", uL, [Y("div", null, [t[21] ||= Y("strong", null, "Connected satellites are current", -1), Y("span", null, V(k.value.length ? "No newer firmware is available for the matched devices." : "Connect a supported satellite to check its installed version."), 1)])])),
				Y("div", dL, [!O.value.length && k.value.length ? (q(), J("button", {
					key: 0,
					class: "tv-button",
					type: "button",
					disabled: !!u.value,
					onClick: t[1] ||= (e) => ke(k.value)
				}, "Reinstall All Connected (" + V(k.value.length) + ")", 9, fL)) : Z("", !0), Y("button", {
					class: "tv-button",
					type: "button",
					disabled: !!u.value,
					onClick: ge
				}, "Manual install or recovery", 8, pL)])
			]),
			f.value || p.value.length ? (q(), J("article", {
				key: 1,
				class: B(["tm-form-card tvoice-progress-dock", `state-${ce.value}`])
			}, [Y("div", null, [Y("span", mL, V(ie.value ? "↻" : ce.value === "failed" ? "!" : "✓"), 1), Y("div", null, [Y("strong", null, V(v.value), 1), Y("small", null, [X(V(f.value?.message || le.value), 1), y.value ? (q(), J(K, { key: 0 }, [X(" · " + V(y.value), 1)], 64)) : Z("", !0)])])]), Y("div", null, [Y("span", null, V(Math.round(se.value)) + "%", 1), Y("button", {
				class: "tv-button",
				type: "button",
				onClick: ve
			}, "View progress")])], 2)) : Z("", !0),
			ga(kl, {
				open: g.value,
				"backdrop-class": "tv-modal-backdrop tset-modal",
				onClose: t[11] ||= (e) => g.value = !1
			}, {
				default: Sn(() => [Y("section", hL, [
					Y("header", null, [t[22] ||= Y("div", null, [
						Y("span", { class: "tv-eyebrow" }, "Install or recover"),
						Y("h2", { id: "tvoice-flasher-title" }, "Manual firmware tools"),
						Y("p", null, "Choose a connection method, then select the exact satellite or hardware family.")
					], -1), Y("button", {
						class: "tv-button",
						type: "button",
						onClick: t[2] ||= (e) => g.value = !1
					}, "Close")]),
					Y("div", gL, [
						Y("button", {
							type: "button",
							class: B({ active: i.value === "ota" }),
							onClick: t[3] ||= (e) => pe("ota")
						}, [...t[23] ||= [Y("b", null, "OTA", -1), Y("span", null, "Update a connected satellite over the network.", -1)]], 2),
						Y("button", {
							type: "button",
							class: B({ active: i.value === "local_usb" }),
							onClick: t[4] ||= (e) => pe("local_usb")
						}, [...t[24] ||= [Y("b", null, "Local USB", -1), Y("span", null, "Flash a device connected directly to this Tater host.", -1)]], 2),
						Y("button", {
							type: "button",
							class: B({ active: i.value === "browser_usb" }),
							onClick: t[5] ||= (e) => pe("browser_usb")
						}, [...t[25] ||= [Y("b", null, "Browser USB", -1), Y("span", null, "Prepare an image for the secure web flasher.", -1)]], 2)
					]),
					Y("section", _L, [
						Y("header", null, [t[27] ||= Y("span", null, "1", -1), Y("div", null, [Y("h3", null, V(i.value === "ota" ? "Choose a connected satellite" : "Choose the hardware family"), 1), t[26] ||= Y("p", null, "Tater only shows targets compatible with this install method.", -1)])]),
						Y("div", vL, [(q(!0), J(K, null, G(P.value, (e) => (q(), J("button", {
							key: Vt(eL)(e),
							type: "button",
							class: B({
								active: (i.value === "ota" ? o.value : a.value) === Vt(eL)(e),
								"no-image": !e.hero_image_src
							}),
							onClick: (t) => me(Vt(eL)(e))
						}, [
							e.hero_image_src ? (q(), J("img", {
								key: 0,
								src: String(e.hero_image_src),
								alt: String(e.hero_image_alt || Vt(tL)(e))
							}, null, 8, bL)) : Z("", !0),
							Y("span", null, [Y("b", null, V(e.title || Vt(tL)(e)), 1), Y("small", null, V(e.detail || e.host || e.template_key || "Official Tater firmware"), 1)]),
							t[28] ||= Y("i", null, null, -1)
						], 10, yL))), 128))]),
						P.value.length ? Z("", !0) : (q(), J("div", xL, V(w.value.empty_message || "No compatible firmware target is available."), 1))
					]),
					i.value !== "ota" && ne.value.path ? (q(), J("section", SL, [t[31] ||= Y("header", null, [Y("span", null, "2"), Y("div", null, [Y("h3", null, "Choose what to preserve"), Y("p", null, "A factory image is best for recovery; keep settings for a normal reinstall.")])], -1), Y("div", CL, [Y("button", {
						type: "button",
						class: B({ active: s.value === "factory" }),
						onClick: t[6] ||= (e) => s.value = "factory"
					}, [...t[29] ||= [Y("b", null, "Factory · erase settings", -1), Y("span", null, "Clean recovery image and fresh setup.", -1)]], 2), Y("button", {
						type: "button",
						class: B({ active: s.value === "ota" }),
						onClick: t[7] ||= (e) => s.value = "ota"
					}, [...t[30] ||= [Y("b", null, "Keep settings", -1), Y("span", null, "Install firmware without clearing provisioning.", -1)]], 2)])])) : Z("", !0),
					i.value === "local_usb" && R.value !== "amlogic_usb_burn" ? (q(), J("section", wL, [
						Y("header", null, [Y("span", null, V(ne.value.path ? "3" : "2"), 1), t[32] ||= Y("div", null, [Y("h3", null, "Select the USB serial port"), Y("p", null, "Connect the satellite with a data cable before refreshing ports.")], -1)]),
						Y("label", TL, [t[34] ||= Y("span", { class: "tm-field-label" }, "Serial port", -1), W(Y("select", { "onUpdate:modelValue": t[8] ||= (e) => l.value = e }, [t[33] ||= Y("option", { value: "" }, "Select a port", -1), (q(!0), J(K, null, G(c.value, (e) => (q(), J("option", {
							key: Vt(eL)(e),
							value: Vt(eL)(e)
						}, V(Vt(tL)(e)), 9, EL))), 128))], 512), [[ds, l.value]])]),
						Y("button", {
							class: "tm-link-button",
							type: "button",
							onClick: t[9] ||= (e) => je()
						}, "Refresh connected ports")
					])) : Z("", !0),
					Object.keys(F.value).length ? (q(), J("section", DL, [F.value.hero_image_src ? (q(), J("img", {
						key: 0,
						src: String(F.value.hero_image_src),
						alt: String(F.value.hero_image_alt || F.value.title)
					}, null, 8, OL)) : Z("", !0), Y("div", null, [
						t[38] ||= Y("span", { class: "tv-eyebrow" }, "Ready target", -1),
						Y("h4", null, V(F.value.title || o.value || a.value), 1),
						Y("p", null, V(F.value.subtitle || F.value.detail), 1),
						Y("dl", kL, [
							t[35] ||= Y("dt", null, "Installed", -1),
							Y("dd", null, V(F.value.installed_firmware_version || "unknown"), 1),
							t[36] ||= Y("dt", null, "Available", -1),
							Y("dd", null, V(I.value.version || F.value.firmware_version || "unknown"), 1),
							t[37] ||= Y("dt", null, "Method", -1),
							Y("dd", null, V(i.value === "ota" ? "Network OTA" : R.value === "amlogic_usb_burn" ? "Amlogic USB" : "ESP serial"), 1)
						])
					])])) : Z("", !0),
					m.value ? (q(), J("div", AL, [Y("div", null, [t[39] ||= Y("strong", null, "Browser image ready", -1), Y("span", null, V(m.value.message), 1)]), m.value.binary_url ? (q(), J("a", {
						key: 0,
						class: "tv-button",
						href: xe(m.value.binary_url),
						target: "_blank",
						rel: "noreferrer"
					}, "Download firmware image", 8, jL)) : Z("", !0)])) : Z("", !0),
					Y("footer", ML, [Y("button", {
						class: "tv-button danger",
						type: "button",
						disabled: !!u.value,
						onClick: Fe
					}, "Clean firmware cache", 8, NL), Y("div", null, [i.value === "ota" ? (q(), J("button", {
						key: 0,
						class: "tv-button primary",
						type: "button",
						disabled: !!u.value || !z.value,
						onClick: t[10] ||= (e) => Oe()
					}, V(u.value === "ota" ? "Starting…" : "Install Latest OTA"), 9, PL)) : i.value === "local_usb" ? (q(), J(K, { key: 1 }, [Y("button", {
						class: "tv-button",
						type: "button",
						disabled: !!u.value,
						onClick: Ne
					}, "Open USB Logs", 8, FL), Y("button", {
						class: "tv-button primary",
						type: "button",
						disabled: !!u.value || !re.value,
						onClick: Me
					}, V(u.value === "local-usb" ? "Starting…" : "Start Local USB Flash"), 9, IL)], 64)) : (q(), J(K, { key: 2 }, [t[40] ||= Y("a", {
						class: "tv-button",
						href: "https://taterassistant.com/usb-flasher/",
						target: "_blank",
						rel: "noreferrer"
					}, "Open Secure Web Flasher ↗", -1), Y("button", {
						class: "tv-button primary",
						type: "button",
						disabled: !!u.value || !re.value,
						onClick: Pe
					}, V(u.value === "browser" ? "Preparing…" : "Prepare Browser USB Image"), 9, LL)], 64))])])
				])]),
				_: 1
			}, 8, ["open"]),
			ga(kl, {
				open: _.value,
				"backdrop-class": "tv-modal-backdrop tset-modal",
				onClose: _e
			}, {
				default: Sn(() => [Y("section", RL, [
					Y("header", null, [Y("div", null, [
						Y("span", zL, V(ie.value ? "Firmware operation in progress" : ce.value === "failed" ? "Firmware operation needs attention" : "Firmware operation complete"), 1),
						Y("h2", BL, V(v.value), 1),
						Y("p", null, V(y.value || "Tater satellite firmware"), 1)
					]), Y("button", {
						class: "tv-button",
						type: "button",
						onClick: _e
					}, V(ie.value ? "Hide" : "Close"), 1)]),
					Y("div", { class: B(["tvoice-progress-hero", `state-${ce.value}`]) }, [Y("div", null, [Y("span", null, V(Math.round(se.value)) + "%", 1), Y("small", null, V(le.value), 1)]), Y("div", null, [
						Y("strong", null, V(ue.value?.title || y.value || "Preparing firmware"), 1),
						Y("p", null, V(f.value?.message || ue.value?.message || "Waiting for firmware output…"), 1),
						Y("progress", {
							value: se.value,
							max: "100"
						}, null, 8, VL)
					])], 2),
					b.value.length ? (q(), J("div", HL, [t[41] ||= Y("header", null, [Y("h3", null, "Update queue"), Y("span", null, "One satellite at a time")], -1), Y("ol", null, [(q(!0), J(K, null, G(b.value, (e, t) => (q(), J("li", {
						key: String(e.key),
						class: B(`state-${e.status}`)
					}, [
						Y("i", null, V(e.status === "completed" ? "✓" : e.status === "running" ? "↻" : e.status === "failed" ? "!" : t + 1), 1),
						Y("span", null, [Y("strong", null, V(e.title), 1), Y("small", null, V(e.message || e.subtitle || (e.status === "queued" ? "Waiting" : e.status)), 1)]),
						Y("b", null, V(e.status === "running" ? `${Math.round(Number(e.progress || 0))}%` : e.status), 1)
					], 2))), 128))])])) : Z("", !0),
					Y("details", UL, [Y("summary", null, [Y("span", null, [t[42] ||= Y("b", null, "Technical details", -1), Y("small", null, V(p.value.length) + " log entr" + V(p.value.length === 1 ? "y" : "ies"), 1)]), Y("span", null, V(f.value?.phase || f.value?.status || "starting"), 1)]), Y("pre", null, V(p.value.map((e) => e.display || e.message || e.text).filter(Boolean).join("\n") || "Waiting for device output…"), 1)]),
					Y("footer", null, [Y("span", null, V(ie.value ? "You can hide this window; the update will keep running." : f.value?.message || le.value), 1), ie.value ? (q(), J("button", {
						key: 0,
						class: "tv-button danger",
						type: "button",
						disabled: !f.value?.session_id,
						onClick: Ae
					}, V(f.value?.session_id ? "Stop" : "Starting…"), 9, WL)) : (q(), J("button", {
						key: 1,
						class: "tv-button primary",
						type: "button",
						onClick: _e
					}, "Done"))])
				])]),
				_: 1
			}, 8, ["open"])
		]));
	}
}), KL = { class: "tm-stack tvoice-airplay" }, qL = {
	key: 0,
	class: "tv-notice error",
	"aria-live": "polite"
}, JL = { class: "tm-form-card tvoice-airplay-hero" }, YL = { class: "tvoice-airplay-summary" }, XL = { class: "tm-form-card tvoice-airplay-control" }, ZL = { class: "tv-toggle tvoice-airplay-toggle" }, QL = ["disabled"], $L = {
	key: 0,
	class: "tv-notice error"
}, eR = { class: "tm-form-card tvoice-airplay-identity" }, tR = { class: "tvoice-airplay-fields" }, nR = ["disabled"], rR = ["disabled"], iR = { class: "tm-form-card tvoice-airplay-destinations" }, aR = { class: "tvoice-section-mark" }, oR = {
	key: 0,
	class: "tvoice-airplay-grid"
}, sR = ["disabled", "onClick"], cR = { class: "tvoice-airplay-target-mark" }, lR = { class: "tvoice-airplay-target-copy" }, uR = { class: "tvoice-airplay-check" }, dR = {
	key: 1,
	class: "tv-notice"
}, fR = { class: "tm-form-card tvoice-save-bar tvoice-airplay-save" }, pR = { class: "tm-inline-actions" }, mR = ["disabled"], hR = ["disabled"], gR = /* @__PURE__ */ sr({
	__name: "VoiceAirPlay",
	props: {
		payload: {},
		actionEndpoint: {}
	},
	emits: ["refresh", "notify"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ Et({
			enabled: !1,
			receiver_name: "Tater Audio",
			receiver_pin: "",
			targets: []
		}), a = /* @__PURE__ */ U(!1), o = /* @__PURE__ */ U(""), s = /* @__PURE__ */ U(""), c = Q(() => n.payload.airplay_input && typeof n.payload.airplay_input == "object" ? n.payload.airplay_input : {}), l = Q(() => c.value.settings && typeof c.value.settings == "object" ? c.value.settings : {}), u = Q(() => c.value.status && typeof c.value.status == "object" ? c.value.status : {}), d = Q(() => Array.isArray(c.value.options) ? c.value.options : []), f = Q(() => String(u.value.status || (i.enabled ? "starting" : "disabled")).toLowerCase()), p = Q(() => ({
			disabled: "Off",
			starting: "Starting",
			ready: "Ready for audio",
			buffering: "Buffering",
			receiving: "Receiving audio",
			routing: "Connecting speakers",
			playing: "Playing",
			waiting_for_targets: "Choose speakers",
			dependency_missing: "Shairport Sync needed",
			runtime_unavailable: "Runtime unavailable",
			stopped: "Stopped",
			error: "Needs attention"
		})[f.value] || f.value.replaceAll("_", " ")), m = Q(() => [
			"ready",
			"buffering",
			"receiving",
			"routing",
			"playing"
		].includes(f.value) ? "good" : [
			"error",
			"dependency_missing",
			"runtime_unavailable"
		].includes(f.value) ? "bad" : "quiet"), h = Q(() => !!u.value.input_active), g = Q(() => String(u.value.route_error || u.value.receiver_error || "")), _ = Q(() => g.value ? g.value : h.value ? "Audio is arriving from an Apple device and being routed to the selected speakers." : i.enabled && i.targets.length ? "Open the AirPlay speaker picker on an Apple device and choose this receiver." : "Enable the receiver and choose where incoming audio should play.");
		function v() {
			a.value || (i.enabled = !!l.value.enabled, i.receiver_name = String(l.value.receiver_name || "Tater Audio"), i.receiver_pin = String(l.value.receiver_pin || ""), i.targets = Array.isArray(l.value.targets) ? l.value.targets.map(String) : []);
		}
		function y() {
			a.value = !0, s.value = "";
		}
		function b(e) {
			let t = String(e || "");
			if (!t || o.value) return;
			let n = new Set(i.targets);
			n.has(t) ? n.delete(t) : n.add(t), i.targets = Array.from(n), y();
		}
		function x(e) {
			return {
				satellite: "Satellite",
				stereo: "Stereo pair",
				sonos: "Sonos + AirPlay",
				airplay: "AirPlay speaker"
			}[String(e.kind || "")] || "Speaker";
		}
		function S(e) {
			return {
				satellite: "SAT",
				stereo: "2X",
				sonos: "SO",
				airplay: "AP"
			}[String(e.kind || "")] || "SPK";
		}
		async function C() {
			if (!o.value) {
				o.value = "save", s.value = "";
				try {
					let e = await $I(n.actionEndpoint, "voice_airplay_input_save", { values: {
						enabled: i.enabled,
						receiver_name: i.receiver_name,
						receiver_pin: i.receiver_pin,
						targets: [...i.targets]
					} });
					a.value = !1, r("notify", String(e.message || "AirPlay Input settings saved."), "success"), r("refresh");
				} catch (e) {
					s.value = e instanceof Error ? e.message : "AirPlay Input settings could not be saved.", r("notify", s.value, "error");
				} finally {
					o.value = "";
				}
			}
		}
		async function w() {
			if (!o.value) {
				o.value = "stop", s.value = "";
				try {
					let e = await $I(n.actionEndpoint, "voice_airplay_input_stop");
					r("notify", String(e.message || "AirPlay input stopped."), "success"), r("refresh");
				} catch (e) {
					s.value = e instanceof Error ? e.message : "AirPlay input could not be stopped.", r("notify", s.value, "error");
				} finally {
					o.value = "";
				}
			}
		}
		return On(() => n.payload, v, {
			deep: !0,
			immediate: !0
		}), (e, t) => (q(), J("section", KL, [
			s.value ? (q(), J("div", qL, V(s.value), 1)) : Z("", !0),
			Y("section", JL, [t[4] ||= ba("<div class=\"tvoice-airplay-flow\" aria-hidden=\"true\"><span class=\"source\">AP</span><i></i><span class=\"receiver\">T</span><i></i><span class=\"speaker\">SAT</span></div><div class=\"tvoice-airplay-copy\"><span class=\"tv-eyebrow\">Built into Tater</span><h3>AirPlay into your satellites</h3><p>Send audio from an iPhone, iPad, or Mac to one Tater receiver, then play it across the satellites and speaker groups you choose.</p></div>", 2), Y("span", { class: B(["tvoice-airplay-status", m.value]) }, [t[3] ||= Y("i", null, null, -1), X(V(p.value), 1)], 2)]),
			Y("section", YL, [
				Y("article", null, [
					t[5] ||= Y("span", null, "Receiver", -1),
					Y("strong", null, V(i.receiver_name || "Tater Audio"), 1),
					t[6] ||= Y("small", null, "Shown in Apple’s speaker picker", -1)
				]),
				Y("article", null, [
					t[7] ||= Y("span", null, "Destinations", -1),
					Y("strong", null, V(i.targets.length), 1),
					Y("small", null, V(i.targets.length === 1 ? "Speaker selected" : "Speakers selected"), 1)
				]),
				Y("article", null, [
					t[8] ||= Y("span", null, "Input", -1),
					Y("strong", null, V(h.value ? "Live" : "Waiting"), 1),
					Y("small", null, V(h.value ? "Audio is arriving now" : "No active stream"), 1)
				])
			]),
			Y("section", XL, [
				Y("header", null, [Y("div", null, [
					t[9] ||= Y("span", { class: "tv-eyebrow" }, "Receiver availability", -1),
					t[10] ||= Y("h3", null, "Make Tater discoverable", -1),
					Y("p", null, V(_.value), 1)
				]), t[11] ||= Y("span", { class: "tvoice-section-mark" }, "ON", -1)]),
				Y("label", ZL, [W(Y("input", {
					"onUpdate:modelValue": t[0] ||= (e) => i.enabled = e,
					class: "tv-checkbox",
					type: "checkbox",
					disabled: !!o.value,
					onChange: y
				}, null, 40, QL), [[cs, i.enabled]]), t[12] ||= Y("span", null, [Y("strong", null, "Enable AirPlay Input"), Y("small", null, "Advertise this Tater server as an AirPlay audio destination.")], -1)]),
				g.value ? (q(), J("div", $L, V(g.value), 1)) : Z("", !0)
			]),
			Y("section", eR, [t[17] ||= Y("header", null, [Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "How Apple devices see Tater"),
				Y("h3", null, "Receiver identity"),
				Y("p", null, "Give the receiver a recognizable room or household name. A PIN is optional.")
			]), Y("span", { class: "tvoice-section-mark" }, "ID")], -1), Y("div", tR, [Y("label", null, [
				t[13] ||= Y("span", null, "Receiver name", -1),
				W(Y("input", {
					"onUpdate:modelValue": t[1] ||= (e) => i.receiver_name = e,
					type: "text",
					maxlength: "80",
					placeholder: "Tater Audio",
					disabled: !!o.value,
					onInput: y
				}, null, 40, nR), [[$, i.receiver_name]]),
				t[14] ||= Y("small", null, "For example: Tater Audio or Whole Home Tater.", -1)
			]), Y("label", null, [
				t[15] ||= Y("span", null, [X("Pairing PIN "), Y("em", null, "Optional")], -1),
				W(Y("input", {
					"onUpdate:modelValue": t[2] ||= (e) => i.receiver_pin = e,
					type: "password",
					maxlength: "4",
					inputmode: "numeric",
					pattern: "[0-9]*",
					placeholder: "Four digits",
					disabled: !!o.value,
					onInput: y
				}, null, 40, rR), [[$, i.receiver_pin]]),
				t[16] ||= Y("small", null, "Leave blank for normal AirPlay pairing.", -1)
			])])]),
			Y("section", iR, [Y("header", null, [t[18] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Playback routing"),
				Y("h3", null, "Choose where AirPlay plays"),
				Y("p", null, "Select one or more destinations. Tater handles the live stream and synchronized routing.")
			], -1), Y("span", aR, V(i.targets.length), 1)]), d.value.length ? (q(), J("div", oR, [(q(!0), J(K, null, G(d.value, (e) => (q(), J("button", {
				key: String(e.value),
				type: "button",
				class: B({
					selected: i.targets.includes(String(e.value)),
					offline: e.available === !1
				}),
				disabled: !!o.value,
				onClick: (t) => b(e.value)
			}, [
				Y("span", cR, V(S(e)), 1),
				Y("span", lR, [
					Y("small", null, V(x(e)), 1),
					Y("strong", null, V(e.label), 1),
					Y("em", null, V(e.description), 1)
				]),
				Y("span", uR, V(i.targets.includes(String(e.value)) ? "✓" : "+"), 1)
			], 10, sR))), 128))])) : (q(), J("div", dR, "No compatible destinations are available yet. Pair a satellite or create a stereo pair first."))]),
			Y("footer", fR, [Y("div", null, [Y("strong", null, V(a.value ? "AirPlay changes are ready to save" : "AirPlay Input is up to date"), 1), t[19] ||= Y("small", null, "Settings apply immediately and remain available without Music Core.", -1)]), Y("div", pR, [h.value ? (q(), J("button", {
				key: 0,
				class: "tv-button danger",
				type: "button",
				disabled: !!o.value,
				onClick: w
			}, V(o.value === "stop" ? "Stopping…" : "Stop Current Input"), 9, mR)) : Z("", !0), Y("button", {
				class: "tv-button primary",
				type: "button",
				disabled: !!o.value || !a.value,
				onClick: C
			}, V(o.value === "save" ? "Saving…" : "Save AirPlay Input"), 9, hR)])])
		]));
	}
}), _R = { class: "tm-stack tvoice-platform" }, vR = {
	key: 0,
	class: "tv-notice error"
}, yR = {
	key: 1,
	class: "tv-notice"
}, bR = { class: "tm-form-card tvoice-subhero tvoice-platform-hero" }, xR = { class: "tvoice-subhero-metrics" }, SR = {
	key: 2,
	class: "tm-stack tvoice-platform-group"
}, CR = { class: "tvoice-section-heading" }, wR = { class: "tvoice-card-identity" }, TR = { class: "tvoice-group-count" }, ER = {
	key: 3,
	class: "tm-stack tvoice-platform-group"
}, DR = { class: "tvoice-section-heading" }, OR = { class: "tvoice-card-identity" }, kR = { class: "tvoice-group-count" }, AR = {
	key: 4,
	class: "tv-notice"
}, jR = {
	key: 5,
	class: "tset-save-bar tvoice-save-bar"
}, MR = { class: "tm-inline-actions" }, NR = ["disabled"], PR = ["disabled"], FR = /* @__PURE__ */ sr({
	__name: "VoicePlatform",
	props: {
		payload: {},
		actionEndpoint: {}
	},
	emits: ["refresh", "notify"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ Et({}), a = /* @__PURE__ */ Et({}), o = /* @__PURE__ */ U(!1), s = /* @__PURE__ */ U(""), c = /* @__PURE__ */ U(""), l = /* @__PURE__ */ U(""), u = Q(() => YI(n.payload, "global_satellite_settings")[0] || null), d = Q(() => YI(n.payload, "settings")[0] || null), f = Q(() => QI(u.value)), p = Q(() => QI(d.value)), m = Q(() => [...f.value, ...p.value].reduce((e, t) => e + (Array.isArray(t.fields) ? t.fields.length : 0), 0));
		function h(e, t) {
			Object.keys(e).forEach((t) => delete e[t]), Object.assign(e, t);
		}
		function g(e = !1) {
			o.value && !e || (h(i, ZI(QI(u.value))), h(a, ZI(QI(d.value))));
		}
		function _(e, t, n) {
			e[t] = n, o.value = !0, c.value = "", l.value = "";
		}
		async function v() {
			if (!s.value) {
				s.value = "save", c.value = "", l.value = "";
				try {
					let e = [];
					if (u.value) {
						let t = await $I(n.actionEndpoint, String(u.value.save_action || "voice_global_satellite_settings_save"), {
							id: u.value.id,
							values: { ...i }
						});
						t.message && e.push(String(t.message));
					}
					if (d.value) {
						let t = await $I(n.actionEndpoint, String(d.value.save_action || "voice_settings_save"), {
							id: d.value.id,
							values: { ...a }
						});
						t.message && e.push(String(t.message));
					}
					o.value = !1, l.value = e.join(" ") || "Voice settings saved and applied.", r("notify", l.value, "success"), r("refresh");
				} catch (e) {
					c.value = e instanceof Error ? e.message : "Voice settings could not be saved.", r("notify", c.value, "error");
				} finally {
					s.value = "";
				}
			}
		}
		async function y() {
			if (!(!d.value || !window.confirm(String(d.value.reset_confirm || "Restore native voice settings to defaults?")))) {
				s.value = "reset", c.value = "";
				try {
					let e = await $I(n.actionEndpoint, String(d.value.reset_action || "voice_settings_reset_defaults"));
					o.value = !1, r("notify", String(e.message || "Voice settings restored to defaults."), "success"), r("refresh");
				} catch (e) {
					c.value = e instanceof Error ? e.message : "Voice defaults could not be restored.", r("notify", c.value, "error");
				} finally {
					s.value = "";
				}
			}
		}
		return On(() => n.payload, () => g(!1), {
			deep: !0,
			immediate: !0
		}), (e, t) => (q(), J("section", _R, [
			c.value ? (q(), J("div", vR, V(c.value), 1)) : Z("", !0),
			l.value ? (q(), J("div", yR, V(l.value), 1)) : Z("", !0),
			Y("section", bR, [t[4] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Shared satellite behavior"),
				Y("h3", null, "Tune the whole voice network"),
				Y("p", null, "Every control is grouped by purpose. Save once at the bottom to apply shared behavior and runtime tuning together.")
			], -1), Y("div", xR, [Y("span", null, [Y("b", null, V(m.value), 1), t[2] ||= X("Controls", -1)]), Y("span", null, [Y("b", null, V(f.value.length + p.value.length), 1), t[3] ||= X("Groups", -1)])])]),
			u.value ? (q(), J("section", SR, [Y("div", CR, [Y("div", wR, [t[6] ||= Y("span", { class: "tvoice-card-mark" }, "ALL", -1), Y("div", null, [
				t[5] ||= Y("span", { class: "tv-eyebrow" }, "Every satellite", -1),
				Y("h3", null, V(u.value.title || "Shared satellite behavior"), 1),
				Y("p", null, V(u.value.subtitle), 1)
			])]), Y("span", TR, V(f.value.length) + " groups", 1)]), ga(Vk, {
				sections: f.value,
				values: i,
				disabled: !!s.value,
				onChange: t[0] ||= (e, t) => _(i, e, t)
			}, null, 8, [
				"sections",
				"values",
				"disabled"
			])])) : Z("", !0),
			d.value ? (q(), J("section", ER, [Y("div", DR, [Y("div", OR, [t[8] ||= Y("span", { class: "tvoice-card-mark" }, "SYS", -1), Y("div", null, [
				t[7] ||= Y("span", { class: "tv-eyebrow" }, "Tater runtime", -1),
				Y("h3", null, V(d.value.title || "Voice pipeline"), 1),
				Y("p", null, V(d.value.subtitle), 1)
			])]), Y("span", kR, V(p.value.length) + " groups", 1)]), ga(Vk, {
				sections: p.value,
				values: a,
				disabled: !!s.value,
				onChange: t[1] ||= (e, t) => _(a, e, t)
			}, null, 8, [
				"sections",
				"values",
				"disabled"
			])])) : Z("", !0),
			!u.value && !d.value ? (q(), J("div", AR, "Satellite runtime settings are unavailable.")) : Z("", !0),
			u.value || d.value ? (q(), J("footer", jR, [Y("div", null, [Y("strong", null, V(o.value ? "Unsaved satellite changes" : "Satellite settings are synchronized"), 1), t[9] ||= Y("span", null, "Saves runtime tuning and immediately applies shared behavior to connected satellites.", -1)]), Y("div", MR, [d.value?.reset_action ? (q(), J("button", {
				key: 0,
				class: "tv-button danger",
				type: "button",
				disabled: !!s.value,
				onClick: y
			}, "Restore defaults", 8, NR)) : Z("", !0), Y("button", {
				class: "tv-button primary",
				type: "button",
				disabled: !!s.value,
				onClick: v
			}, V(s.value === "save" ? "Applying…" : "Save & Apply Satellite Settings"), 9, PR)])])) : Z("", !0)
		]));
	}
}), IR = { class: "tm-stack tvoice-presence tpresence-next" }, LR = { class: "tm-form-card tvoice-presence-hero tpresence-hero" }, RR = {
	class: "tpresence-nav",
	"aria-label": "Presence views"
}, zR = ["onClick"], BR = { class: "tm-metrics tvoice-presence-metrics tpresence-metrics" }, VR = { class: "home" }, HR = { class: "away" }, UR = {
	key: 0,
	class: "tv-notice error",
	"aria-live": "polite"
}, WR = {
	key: 1,
	class: "tv-notice"
}, GR = { class: "tm-form-card tvoice-presence-board tpresence-location-board" }, KR = { class: "tvoice-presence-board-head" }, qR = { class: "tvoice-presence-filters" }, JR = ["value"], YR = {
	key: 0,
	class: "tvoice-presence-rooms tpresence-house"
}, XR = { class: "tvoice-presence-devices" }, ZR = ["onClick"], QR = { class: "tpresence-device-icon" }, $R = { class: "tvoice-presence-device-copy" }, ez = ["title"], tz = ["onClick"], nz = {
	key: 1,
	class: "tv-empty"
}, rz = {
	key: 0,
	class: "tm-form-card tvoice-presence-detail tpresence-detail"
}, iz = { class: "tpresence-detail-actions" }, az = { class: "tvoice-presence-detail-grid" }, oz = { class: "tvoice-presence-source-list" }, sz = {
	key: 3,
	class: "tm-form-card tpresence-device-registry"
}, cz = {
	key: 0,
	class: "tpresence-registry-grid"
}, lz = ["onClick"], uz = { class: "tpresence-device-icon" }, dz = {
	key: 1,
	class: "tv-empty"
}, fz = { class: "tpresence-discovered" }, pz = { class: "tpresence-discovered-body" }, mz = { class: "tpresence-registry-search" }, hz = {
	key: 0,
	class: "tpresence-registry-grid unassigned"
}, gz = ["onClick"], _z = { class: "tpresence-device-icon" }, vz = {
	key: 1,
	class: "tv-empty"
}, yz = {
	key: 4,
	class: "tm-form-card tpresence-history"
}, bz = ["disabled"], xz = {
	key: 0,
	class: "tpresence-timeline"
}, Sz = {
	key: 1,
	class: "tv-empty"
}, Cz = {
	key: 5,
	class: "tpresence-calibration-grid"
}, wz = { class: "tm-form-card tpresence-calibration" }, Tz = { class: "tpresence-form-grid" }, Ez = ["disabled"], Dz = { class: "tm-form-card tpresence-scanner-calibration" }, Oz = { key: 0 }, kz = ["onUpdate:modelValue"], Az = ["disabled", "onClick"], jz = {
	key: 1,
	class: "tv-empty"
}, Mz = { class: "tvoice-presence-lower tpresence-lower" }, Nz = { class: "tm-form-card tvoice-presence-api" }, Pz = { class: "tm-form-card tpresence-diagnostics" }, Fz = {
	key: 0,
	class: "tv-modal tvoice-presence-room-modal",
	role: "dialog",
	"aria-modal": "true"
}, Iz = { class: "tvoice-presence-room-modal-title" }, Lz = { class: "tvoice-presence-room-modal-body" }, Rz = ["onClick"], zz = { class: "tpresence-device-icon" }, Bz = { class: "tvoice-presence-device-copy" }, Vz = {
	key: 0,
	class: "tv-modal tpresence-editor",
	role: "dialog",
	"aria-modal": "true",
	"aria-labelledby": "tpresence-editor-title"
}, Hz = { id: "tpresence-editor-title" }, Uz = { class: "tpresence-editor-body" }, Wz = { class: "tpresence-form-grid" }, Gz = { class: "tpresence-check" }, Kz = { class: "tpresence-form-grid" }, qz = { class: "wide" }, Jz = {
	key: 0,
	class: "tpresence-identity-chips"
}, Yz = { class: "tpresence-form-grid" }, Xz = ["disabled"], Zz = ["disabled"], Qz = 6, $z = /* @__PURE__ */ sr({
	__name: "VoicePresence",
	props: {
		snapshotEndpoint: {},
		eventsEndpoint: {}
	},
	emits: ["notify"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ U({
			devices: [],
			sources: [],
			rooms: {},
			history: [],
			settings: {},
			diagnostics: {}
		}), a = /* @__PURE__ */ U({
			settings: {},
			devices: [],
			scanners: {},
			capabilities: {}
		}), o = /* @__PURE__ */ U(!0), s = /* @__PURE__ */ U(!1), c = /* @__PURE__ */ U(""), l = /* @__PURE__ */ U("connecting"), u = /* @__PURE__ */ U("locations"), d = /* @__PURE__ */ U(""), f = /* @__PURE__ */ U(""), p = /* @__PURE__ */ U("all"), m = /* @__PURE__ */ U(""), h = /* @__PURE__ */ U(""), g = /* @__PURE__ */ U(!1), _ = /* @__PURE__ */ U(Date.now() / 1e3), v = /* @__PURE__ */ U(0), y = /* @__PURE__ */ U(ae()), b = /* @__PURE__ */ U({}), x = /* @__PURE__ */ U({}), S = null, C = 0, w = 0, T = Q(() => `${n.snapshotEndpoint.replace(/\/$/, "")}/config`), E = Q(() => Array.isArray(i.value.devices) ? i.value.devices : []), D = Q(() => Array.isArray(i.value.sources) ? i.value.sources : []), O = Q(() => Array.isArray(i.value.history) ? i.value.history : []), k = Q(() => E.value.filter((e) => !!e.tracked)), A = Q(() => E.value.filter((e) => !!e.configured).slice().sort(me)), j = Q(() => {
			let e = f.value.trim().toLowerCase();
			return E.value.filter((e) => !e.configured).filter((t) => !e || [
				ce(t),
				t.address,
				t.advertised_name
			].some((t) => String(t || "").toLowerCase().includes(e))).slice().sort(me);
		}), M = Q(() => k.value.filter((e) => e.home_state === "home").length), N = Q(() => k.value.filter((e) => e.home_state === "away").length), P = Q(() => D.value.filter((e) => le(e.last_seen_ts) <= 15).length), F = Q(() => E.value.filter((e) => !!e.present).length), I = Q(() => [...new Set(E.value.filter((e) => e.home_state !== "away").map((e) => se(e)).filter((e) => e && e !== "Away"))].sort((e, t) => e === "Unknown" ? 1 : t === "Unknown" ? -1 : e.localeCompare(t))), ee = Q(() => {
			let e = d.value.trim().toLowerCase();
			return E.value.filter((t) => {
				let n = se(t);
				return p.value !== "all" && n !== p.value ? !1 : !e || [
					t.display_name,
					t.advertised_name,
					t.address,
					t.owner,
					t.category,
					n
				].some((t) => String(t || "").toLowerCase().includes(e));
			});
		}), te = Q(() => I.value.filter((e) => p.value === "all" || e === p.value).map((e) => ({
			room: e,
			devices: ee.value.filter((t) => t.home_state !== "away" && se(t) === e).sort(pe)
		})).filter((e) => e.devices.length || p.value !== "all")), ne = Q(() => te.value.find((e) => e.room === h.value) || null), L = Q(() => E.value.find((e) => oe(e) === m.value) || null), R = Q(() => i.value.diagnostics || {}), z = Q(() => a.value.settings || i.value.settings || {}), re = Q(() => a.value.capabilities || {}), ie = Q(() => ({
			connecting: "Connecting",
			live: "Live",
			reconnecting: "Reconnecting",
			unsupported: "Polling"
		})[l.value]);
		function ae() {
			return {
				id: "",
				configured: !1,
				address: "",
				name: "",
				owner: "",
				category: "device",
				track: !0,
				referencePower: "",
				maxRadius: "",
				homeTimeout: "",
				ibeaconId: "",
				irk: "",
				identities: []
			};
		}
		function oe(e) {
			return String(e.presence_id || e.id || e.address || "");
		}
		function se(e) {
			return String(e.location_room || e.location || e.strongest_room || "Unknown").trim() || "Unknown";
		}
		function ce(e) {
			return String(e.display_name || e.advertised_name || `BLE ${String(e.address || "").slice(-8).toUpperCase()}`);
		}
		function le(e) {
			let t = Number(e || 0);
			return t > 0 ? Math.max(0, _.value - t) : Infinity;
		}
		function ue(e) {
			let t = le(e);
			return Number.isFinite(t) ? t < 2 ? "now" : t < 60 ? `${Math.floor(t)}s ago` : t < 3600 ? `${Math.floor(t / 60)}m ago` : t < 86400 ? `${Math.floor(t / 3600)}h ago` : `${Math.floor(t / 86400)}d ago` : "never";
		}
		function de(e) {
			let t = Number(e);
			return !Number.isFinite(t) || t <= 0 || t >= 999 ? "Distance unavailable" : t < 1 ? `${Math.round(t * 100)} cm away` : `~${t < 10 ? t.toFixed(1) : Math.round(t)} m away`;
		}
		function fe(e) {
			let t = Number(e ?? -127);
			return t >= -55 ? 4 : t >= -67 ? 3 : t >= -78 ? 2 : 1;
		}
		function pe(e, t) {
			let n = Number(e.distance_m ?? 999), r = Number(t.distance_m ?? 999);
			return Math.abs(n - r) >= .35 ? n - r : ce(e).localeCompare(ce(t));
		}
		function me(e, t) {
			return ce(e).localeCompare(ce(t), void 0, {
				numeric: !0,
				sensitivity: "base"
			}) || oe(e).localeCompare(oe(t), void 0, {
				numeric: !0,
				sensitivity: "base"
			});
		}
		function he(e) {
			return e === "Unknown" ? "?" : e.split(/\s+/).map((e) => e[0] || "").join("").slice(0, 2).toUpperCase();
		}
		function ge(e) {
			let t = String(e.category || "").toLowerCase();
			return t.includes("phone") ? "▯" : t.includes("watch") ? "◉" : t.includes("person") ? "●" : t.includes("pet") ? "◆" : e.ibeacon || t.includes("beacon") ? "⌁" : "•";
		}
		function _e(e) {
			return e.type === "arrived_home" ? "↘" : e.type === "left_home" ? "↗" : "→";
		}
		function ve(e) {
			return e.type === "arrived_home" ? `Arrived home${e.room ? ` · ${e.room}` : ""}` : e.type === "left_home" ? "Left home" : `${e.from || "Unknown"} → ${e.to || e.room || "Unknown"}`;
		}
		function ye(e) {
			let t = Array.isArray(e.identities) ? e.identities : [];
			return t.length ? t.map((e) => String(e.type || "BLE").toUpperCase()).join(" + ") : String(e.address || "Unidentified BLE device");
		}
		function be(e) {
			i.value = e;
			let t = { ...x.value };
			(Array.isArray(e.sources) ? e.sources : []).forEach((e) => {
				let n = String(e.selector || "");
				n && t[n] === void 0 && (t[n] = String(e.rssi_offset_db ?? 0));
			}), x.value = t, v.value = Date.now() / 1e3, o.value = !1, c.value = "", m.value && !E.value.some((e) => oe(e) === m.value) && (m.value = "");
		}
		function xe(e) {
			let t = new URL(e, window.location.href);
			return t.searchParams.set("limit", "500"), t.searchParams.set("max_age_s", "60"), t.searchParams.set("include_observations", "false"), t.searchParams.set("history_limit", "100"), t.toString();
		}
		async function H(e = !1) {
			e || (o.value = !0);
			try {
				be(await As(xe(n.snapshotEndpoint)));
			} catch (e) {
				c.value = e instanceof Error ? e.message : "Presence data could not be loaded.", o.value = !1;
			}
		}
		async function Se() {
			try {
				let e = await As(T.value);
				a.value = e, b.value = { ...e.settings || {} };
				let t = {};
				Object.entries(e.scanners || {}).forEach(([e, n]) => {
					t[e] = String(n?.rssi_offset_db ?? 0);
				}), D.value.forEach((e) => {
					let n = String(e.selector || "");
					n && t[n] === void 0 && (t[n] = String(e.rssi_offset_db ?? 0));
				}), x.value = t;
			} catch (e) {
				c.value = e instanceof Error ? e.message : "Presence configuration could not be loaded.";
			}
		}
		async function Ce(e, t = {}) {
			s.value = !0;
			try {
				return a.value = await js(T.value, {
					action: e,
					payload: t
				}), b.value = { ...a.value.settings || {} }, await H(!0), r("notify", "Tater Presence updated.", "success"), !0;
			} catch (e) {
				return r("notify", e instanceof Error ? e.message : "Presence update failed.", "error"), !1;
			} finally {
				s.value = !1;
			}
		}
		function we(e, t = !1) {
			let n = oe(e);
			m.value = m.value === n ? "" : n, t && (h.value = "");
		}
		function Te(e) {
			let t = Array.isArray(e.identities) ? e.identities.map((e) => ({ ...e })) : [], n = t.find((e) => e.type === "ibeacon");
			y.value = {
				id: oe(e),
				configured: !!e.configured,
				address: String(e.address || ""),
				name: ce(e),
				owner: String(e.owner || ""),
				category: String(e.category || "device"),
				track: !e.configured || !!e.tracked,
				referencePower: e.configured && e.reference_power_override != null ? String(e.reference_power_override) : "",
				maxRadius: e.configured && e.max_radius_m_override != null ? String(e.max_radius_m_override) : "",
				homeTimeout: e.configured && e.home_timeout_s_override != null ? String(e.home_timeout_s_override) : "",
				ibeaconId: String(n?.value || e.ibeacon?.id || ""),
				irk: "",
				identities: t
			}, g.value = !0;
		}
		async function Ee() {
			let e = y.value, t = e.identities.filter((t) => t.type !== "ibeacon" || !e.ibeaconId.trim()).map((e) => ({
				type: e.type,
				value: e.value,
				fingerprint: e.fingerprint
			}));
			e.ibeaconId.trim() && t.push({
				type: "ibeacon",
				value: e.ibeaconId.trim()
			}), e.irk.trim() && t.push({
				type: "irk",
				value: e.irk.trim()
			}), !t.some((e) => e.type === "address") && e.address && t.push({
				type: "address",
				value: e.address
			}), await Ce("upsert_device", {
				id: e.id,
				address: e.address,
				name: e.name,
				owner: e.owner,
				category: e.category,
				track: e.track,
				reference_power: e.referencePower === "" ? null : Number(e.referencePower),
				max_radius_m: e.maxRadius === "" ? null : Number(e.maxRadius),
				home_timeout_s: e.homeTimeout === "" ? null : Number(e.homeTimeout),
				identities: t
			}) && (g.value = !1, m.value = e.id);
		}
		async function De() {
			y.value.id && await Ce("remove_device", { id: y.value.id }) && (g.value = !1, m.value = "");
		}
		async function Oe() {
			await Ce("save_settings", {
				reference_power: Number(b.value.reference_power),
				attenuation: Number(b.value.attenuation),
				home_timeout_s: Number(b.value.home_timeout_s),
				max_home_radius_m: Number(b.value.max_home_radius_m),
				max_room_radius_m: Number(b.value.max_room_radius_m),
				history_days: Number(b.value.history_days)
			});
		}
		async function ke(e) {
			await Ce("save_scanner", {
				selector: e,
				rssi_offset_db: Number(x.value[e] || 0)
			});
		}
		async function Ae(e, t) {
			try {
				await navigator.clipboard.writeText(new URL(e, window.location.href).toString()), r("notify", `${t} endpoint copied.`, "success");
			} catch {
				r("notify", `Could not copy the ${t.toLowerCase()} endpoint.`, "error");
			}
		}
		function je() {
			S?.close(), S = null;
		}
		function Me() {
			if (je(), typeof window.EventSource != "function") {
				l.value = "unsupported";
				return;
			}
			l.value = "connecting";
			let e = new EventSource(xe(n.eventsEndpoint));
			S = e, e.onopen = () => {
				l.value = "live";
			}, e.addEventListener("presence.snapshot", (e) => {
				try {
					be(JSON.parse(e.data)), l.value = "live";
				} catch {
					c.value = "A live presence update could not be read.";
				}
			}), e.addEventListener("presence.event", () => {
				H(!0);
			}), e.onerror = () => {
				l.value = "reconnecting";
			};
		}
		function Ne() {
			document.visibilityState === "visible" && (H(!0), S || Me());
		}
		return Er(() => {
			Promise.all([H(), Se()]), Me(), C = window.setInterval(() => {
				document.visibilityState === "visible" && l.value !== "live" && H(!0);
			}, 5e3), w = window.setInterval(() => {
				_.value = Date.now() / 1e3;
			}, 1e3), document.addEventListener("visibilitychange", Ne);
		}), kr(() => {
			je(), window.clearInterval(C), window.clearInterval(w), document.removeEventListener("visibilitychange", Ne);
		}), (t, n) => (q(), J("section", IR, [
			Y("section", LR, [
				n[31] ||= Y("div", {
					class: "tvoice-presence-radar",
					"aria-hidden": "true"
				}, [
					Y("i"),
					Y("i"),
					Y("span")
				], -1),
				n[32] ||= Y("div", { class: "tvoice-presence-copy" }, [
					Y("span", { class: "tv-eyebrow" }, "Tater Presence Intelligence"),
					Y("h3", null, "Know what is home—and which room it is in"),
					Y("p", null, "Native satellites resolve stable BLE identities, calibrated distance, room movement, and home or away state locally inside Tater.")
				], -1),
				Y("div", { class: B(["tvoice-presence-live", `state-${l.value}`]) }, [n[30] ||= Y("i", null, null, -1), Y("span", null, [Y("strong", null, V(ie.value), 1), Y("small", null, V(v.value ? `Updated ${ue(v.value)}` : "Waiting for data"), 1)])], 2)
			]),
			Y("nav", RR, [(q(), J(K, null, G([
				["locations", "Locations"],
				["devices", "Tracked devices"],
				["history", "History"],
				["calibration", "Calibration"]
			], (e) => Y("button", {
				key: e[0],
				type: "button",
				class: B({ active: u.value === e[0] }),
				onClick: (t) => u.value = e[0]
			}, V(e[1]), 11, zR)), 64))]),
			Y("div", BR, [
				Y("article", VR, [
					n[33] ||= Y("span", null, "Home", -1),
					Y("strong", null, V(M.value), 1),
					n[34] ||= Y("small", null, "tracked devices", -1)
				]),
				Y("article", HR, [
					n[35] ||= Y("span", null, "Away", -1),
					Y("strong", null, V(N.value), 1),
					n[36] ||= Y("small", null, "tracked devices", -1)
				]),
				Y("article", null, [
					n[37] ||= Y("span", null, "Nearby", -1),
					Y("strong", null, V(F.value), 1),
					n[38] ||= Y("small", null, "seen in 15 seconds", -1)
				]),
				Y("article", null, [
					n[39] ||= Y("span", null, "Rooms", -1),
					Y("strong", null, V(I.value.length), 1),
					n[40] ||= Y("small", null, "with live coverage", -1)
				]),
				Y("article", null, [
					n[41] ||= Y("span", null, "Scanners", -1),
					Y("strong", null, V(P.value) + "/" + V(D.value.length), 1),
					n[42] ||= Y("small", null, "satellites reporting", -1)
				])
			]),
			c.value ? (q(), J("div", UR, V(c.value), 1)) : Z("", !0),
			o.value ? (q(), J("div", WR, "Building the live Tater presence map…")) : u.value === "locations" ? (q(), J(K, { key: 2 }, [Y("section", GR, [Y("header", KR, [n[46] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Live location view"),
				Y("h3", null, "Room-by-room location"),
				Y("p", null, "Estimated distance is calibrated per device and satellite. Tater waits for sustained movement before changing rooms.")
			], -1), Y("div", qR, [Y("label", null, [n[43] ||= Y("span", null, "Search", -1), W(Y("input", {
				"onUpdate:modelValue": n[0] ||= (e) => d.value = e,
				type: "search",
				placeholder: "Device, owner, or address"
			}, null, 512), [[$, d.value]])]), Y("label", null, [n[45] ||= Y("span", null, "Room", -1), W(Y("select", { "onUpdate:modelValue": n[1] ||= (e) => p.value = e }, [n[44] ||= Y("option", { value: "all" }, "All rooms", -1), (q(!0), J(K, null, G(I.value, (e) => (q(), J("option", {
				key: e,
				value: e
			}, V(e), 9, JR))), 128))], 512), [[ds, p.value]])])])]), te.value.length ? (q(), J("div", YR, [(q(!0), J(K, null, G(te.value, (e) => (q(), J("article", {
				key: e.room,
				class: B(["tvoice-presence-room tpresence-room", { unknown: e.room === "Unknown" }])
			}, [
				Y("header", null, [
					Y("span", null, V(he(e.room)), 1),
					Y("div", null, [Y("strong", null, V(e.room), 1), Y("small", null, V(e.devices.length) + " located · nearest first", 1)]),
					n[47] ||= Y("i", null, null, -1)
				]),
				Y("div", XR, [(q(!0), J(K, null, G(e.devices.slice(0, Qz), (e) => (q(), J("button", {
					key: oe(e),
					type: "button",
					class: B({
						selected: m.value === oe(e),
						stale: !e.present,
						tracked: e.tracked
					}),
					onClick: (t) => we(e)
				}, [
					Y("span", QR, V(ge(e)), 1),
					Y("span", $R, [Y("strong", null, V(ce(e)), 1), Y("small", null, [Y("b", null, V(de(e.distance_m)), 1), X(" · " + V(e.owner || ye(e)), 1)])]),
					Y("span", {
						class: "tvoice-presence-signal",
						title: `${e.strongest_rssi} dBm`
					}, [(q(), J(K, null, G(4, (t) => Y("i", {
						key: t,
						class: B({ active: t <= fe(e.strongest_rssi) })
					}, null, 2)), 64)), Y("b", null, V(e.strongest_rssi), 1)], 8, ez)
				], 10, ZR))), 128))]),
				e.devices.length > Qz ? (q(), J("button", {
					key: 0,
					class: "tvoice-presence-room-more",
					type: "button",
					onClick: (t) => h.value = e.room
				}, [
					Y("span", null, "View all " + V(e.devices.length), 1),
					Y("small", null, "+" + V(e.devices.length - Qz) + " more", 1),
					n[48] ||= Y("b", null, "→", -1)
				], 8, tz)) : Z("", !0)
			], 2))), 128))])) : (q(), J("div", nz, "No devices match this view yet. Nearby devices appear automatically as satellites hear them."))]), L.value ? (q(), J("section", rz, [
				Y("header", null, [Y("div", null, [
					n[49] ||= Y("span", { class: "tv-eyebrow" }, "Current location", -1),
					Y("h3", null, V(ce(L.value)), 1),
					Y("p", null, V(L.value.owner || ye(L.value)) + " · " + V(L.value.address || "private identity"), 1)
				]), Y("div", iz, [Y("button", {
					class: "tv-button",
					type: "button",
					onClick: n[2] ||= (e) => Te(L.value)
				}, V(L.value.configured ? "Edit tracker" : "Name & track"), 1), Y("button", {
					class: "tv-button",
					type: "button",
					onClick: n[3] ||= (e) => m.value = ""
				}, "Close")])]),
				Y("div", { class: B(["tpresence-location-callout", String(L.value.home_state || "nearby")]) }, [
					Y("span", null, V(he(se(L.value))), 1),
					Y("div", null, [
						Y("small", null, V(L.value.home_state === "away" ? "Last known location" : "Located in"), 1),
						Y("strong", null, V(se(L.value)), 1),
						Y("p", null, V(de(L.value.distance_m)) + " from " + V(L.value.strongest_selector || "the nearest satellite"), 1)
					]),
					Y("b", null, V(L.value.home_state === "away" ? "Away" : L.value.confidence || "Live"), 1)
				], 2),
				Y("div", az, [
					Y("div", null, [
						n[50] ||= Y("span", null, "Home state", -1),
						Y("strong", null, V(L.value.home_state || "nearby"), 1),
						Y("small", null, "changed " + V(ue(L.value.state_changed_ts)), 1)
					]),
					Y("div", null, [
						n[51] ||= Y("span", null, "Estimated distance", -1),
						Y("strong", null, V(de(L.value.distance_m)), 1),
						Y("small", null, "RSSI " + V(L.value.calibrated_rssi ?? L.value.strongest_rssi) + " dBm calibrated", 1)
					]),
					Y("div", null, [
						n[52] ||= Y("span", null, "Last seen", -1),
						Y("strong", null, V(ue(L.value.last_seen_ts)), 1),
						Y("small", null, V(L.value.present ? "present now" : "not currently advertising"), 1)
					]),
					Y("div", null, [
						n[53] ||= Y("span", null, "Identity", -1),
						Y("strong", null, V(ye(L.value)), 1),
						Y("small", null, V((L.value.addresses || []).length || 1) + " observed address" + V((L.value.addresses || []).length === 1 ? "" : "es"), 1)
					])
				]),
				Y("div", oz, [(q(!0), J(K, null, G(L.value.sources || [], (e) => (q(), J("article", { key: String(e.selector) }, [
					Y("span", null, V(he(e.room || "Unknown")), 1),
					Y("div", null, [Y("strong", null, V(e.room || e.device_name || "Unknown room"), 1), Y("small", null, V(de(e.distance_m)) + " · offset " + V(Number(e.rssi_offset_db || 0) >= 0 ? "+" : "") + V(e.rssi_offset_db || 0) + " dB", 1)]),
					Y("b", null, V(e.calibrated_rssi ?? e.rssi) + " dBm", 1)
				]))), 128))])
			])) : Z("", !0)], 64)) : u.value === "devices" ? (q(), J("section", sz, [
				Y("header", null, [n[54] ||= Y("div", null, [
					Y("span", { class: "tv-eyebrow" }, "Stable identity registry"),
					Y("h3", null, "Tracked devices"),
					Y("p", null, "This registry stays alphabetized while live room and distance changes remain in Locations.")
				], -1), Y("span", null, V(k.value.length) + " tracking", 1)]),
				A.value.length ? (q(), J("div", cz, [(q(!0), J(K, null, G(A.value, (e) => (q(), J("button", {
					key: oe(e),
					type: "button",
					class: B(["configured", { away: e.home_state === "away" }]),
					onClick: (t) => Te(e)
				}, [
					Y("span", uz, V(ge(e)), 1),
					Y("span", null, [Y("strong", null, V(ce(e)), 1), Y("small", null, V(ye(e)) + " · " + V(e.owner || "No owner"), 1)]),
					Y("b", null, V(e.tracked ? e.home_state === "away" ? "Away" : se(e) : "Paused"), 1),
					n[55] ||= Y("i", null, "Edit", -1)
				], 10, lz))), 128))])) : (q(), J("div", dz, "No devices are tracked yet. Add one from the nearby-device list below.")),
				Y("details", fz, [Y("summary", null, [n[56] ||= Y("span", null, [Y("strong", null, "Add a nearby device"), Y("small", null, "Unassigned devices stay in a stable identity order—not live distance order.")], -1), Y("b", null, V(j.value.length) + " available", 1)]), Y("div", pz, [Y("label", mz, [n[57] ||= Y("span", null, "Find a device", -1), W(Y("input", {
					"onUpdate:modelValue": n[4] ||= (e) => f.value = e,
					type: "search",
					placeholder: "Name or BLE address"
				}, null, 512), [[$, f.value]])]), j.value.length ? (q(), J("div", hz, [(q(!0), J(K, null, G(j.value, (e) => (q(), J("button", {
					key: oe(e),
					type: "button",
					onClick: (t) => Te(e)
				}, [
					Y("span", _z, V(ge(e)), 1),
					Y("span", null, [Y("strong", null, V(ce(e)), 1), Y("small", null, V(ye(e)), 1)]),
					n[58] ||= Y("b", null, "Track", -1)
				], 8, gz))), 128))])) : (q(), J("div", vz, V(f.value ? "No nearby devices match that search." : "No unassigned BLE devices are nearby."), 1))])])
			])) : u.value === "history" ? (q(), J("section", yz, [Y("header", null, [Y("div", null, [
				n[59] ||= Y("span", { class: "tv-eyebrow" }, "Persistent movement history", -1),
				n[60] ||= Y("h3", null, "Arrivals, departures, and room changes", -1),
				Y("p", null, "History stays in Tater for " + V(z.value.history_days || 30) + " days and is available to Cores, Verbas, and native automations.", 1)
			]), O.value.length ? (q(), J("button", {
				key: 0,
				class: "tv-button",
				type: "button",
				disabled: s.value,
				onClick: n[5] ||= (e) => Ce("clear_history")
			}, "Clear history", 8, bz)) : Z("", !0)]), O.value.length ? (q(), J("div", xz, [(q(!0), J(K, null, G(O.value, (e) => (q(), J("article", { key: String(e.id) }, [
				Y("span", null, V(_e(e)), 1),
				Y("div", null, [
					Y("strong", null, V(e.name || e.device_id), 1),
					Y("p", null, V(ve(e)), 1),
					Y("small", null, [e.distance_m ? (q(), J(K, { key: 0 }, [X(V(de(e.distance_m)) + " · ", 1)], 64)) : Z("", !0), X(V(e.confidence || "recorded") + " confidence", 1)])
				]),
				Y("time", null, V(ue(e.at)), 1)
			]))), 128))])) : (q(), J("div", Sz, "Track a device to begin recording room movement and home or away changes."))])) : (q(), J("div", Cz, [Y("section", wz, [
				n[74] ||= Y("header", null, [Y("div", null, [
					Y("span", { class: "tv-eyebrow" }, "Distance model"),
					Y("h3", null, "Whole-home calibration"),
					Y("p", null, "These defaults convert steadied RSSI into an estimated distance. Device-specific values can override them.")
				]), Y("span", null, "Local only")], -1),
				Y("div", Tz, [
					Y("label", null, [
						n[61] ||= Y("span", null, "RSSI at one metre", -1),
						W(Y("input", {
							"onUpdate:modelValue": n[6] ||= (e) => b.value.reference_power = e,
							type: "number",
							min: "-100",
							max: "-20",
							step: "1"
						}, null, 512), [[$, b.value.reference_power]]),
						n[62] ||= Y("small", null, "Place a reference beacon exactly 1 m from a satellite.", -1)
					]),
					Y("label", null, [
						n[63] ||= Y("span", null, "Environmental attenuation", -1),
						W(Y("input", {
							"onUpdate:modelValue": n[7] ||= (e) => b.value.attenuation = e,
							type: "number",
							min: "1",
							max: "6",
							step: "0.1"
						}, null, 512), [[$, b.value.attenuation]]),
						n[64] ||= Y("small", null, "Typical indoor values are 2.2–3.5.", -1)
					]),
					Y("label", null, [
						n[65] ||= Y("span", null, "Room radius", -1),
						W(Y("input", {
							"onUpdate:modelValue": n[8] ||= (e) => b.value.max_room_radius_m = e,
							type: "number",
							min: "1",
							max: "100",
							step: "1"
						}, null, 512), [[$, b.value.max_room_radius_m]]),
						n[66] ||= Y("small", null, "Signals beyond this show as Unknown.", -1)
					]),
					Y("label", null, [
						n[67] ||= Y("span", null, "Home radius", -1),
						W(Y("input", {
							"onUpdate:modelValue": n[9] ||= (e) => b.value.max_home_radius_m = e,
							type: "number",
							min: "1",
							max: "250",
							step: "1"
						}, null, 512), [[$, b.value.max_home_radius_m]]),
						n[68] ||= Y("small", null, "Maximum distance still considered home.", -1)
					]),
					Y("label", null, [
						n[69] ||= Y("span", null, "Away timeout", -1),
						W(Y("input", {
							"onUpdate:modelValue": n[10] ||= (e) => b.value.home_timeout_s = e,
							type: "number",
							min: "15",
							max: "3600",
							step: "15"
						}, null, 512), [[$, b.value.home_timeout_s]]),
						n[70] ||= Y("small", null, "Seconds without a trustworthy observation.", -1)
					]),
					Y("label", null, [
						n[71] ||= Y("span", null, "History retention", -1),
						W(Y("input", {
							"onUpdate:modelValue": n[11] ||= (e) => b.value.history_days = e,
							type: "number",
							min: "1",
							max: "365",
							step: "1"
						}, null, 512), [[$, b.value.history_days]]),
						n[72] ||= Y("small", null, "Days of arrival and movement events.", -1)
					])
				]),
				Y("footer", null, [n[73] ||= Y("span", null, "Distance is an estimate; walls, people, and antenna orientation affect BLE RSSI.", -1), Y("button", {
					class: "tv-button primary",
					type: "button",
					disabled: s.value,
					onClick: Oe
				}, V(s.value ? "Saving…" : "Save model"), 9, Ez)])
			]), Y("section", Dz, [Y("header", null, [n[75] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Receiver normalization"),
				Y("h3", null, "Satellite RSSI offsets"),
				Y("p", null, "Use offsets when one satellite consistently appears stronger or weaker than the others at the same physical distance.")
			], -1), Y("span", null, V(R.value.calibrated_scanners || 0) + " calibrated", 1)]), D.value.length ? (q(), J("div", Oz, [(q(!0), J(K, null, G(D.value, (e) => (q(), J("article", {
				key: String(e.selector),
				class: B({ offline: le(e.last_seen_ts) > 15 })
			}, [
				n[77] ||= Y("i", null, null, -1),
				Y("span", null, [Y("strong", null, V(e.room || e.device_name || e.selector), 1), Y("small", null, V(e.board || e.selector) + " · " + V(ue(e.last_seen_ts)), 1)]),
				Y("label", null, [W(Y("input", {
					"onUpdate:modelValue": (t) => x.value[String(e.selector)] = t,
					type: "number",
					min: "-40",
					max: "40",
					step: "0.5"
				}, null, 8, kz), [[$, x.value[String(e.selector)]]]), n[76] ||= Y("small", null, "dB", -1)]),
				Y("button", {
					class: "tv-button",
					type: "button",
					disabled: s.value,
					onClick: (t) => ke(String(e.selector))
				}, "Save", 8, Az)
			], 2))), 128))])) : (q(), J("div", jz, "Satellite calibration appears after a native scanner reports in."))])])),
			Y("div", Mz, [Y("section", Nz, [
				n[82] ||= Y("header", null, [Y("div", null, [Y("span", { class: "tv-eyebrow" }, "Tater-native API"), Y("h3", null, "Use location anywhere in Tater")]), Y("span", null, "REST + SSE")], -1),
				n[83] ||= Y("p", null, "Cores, Verbas, integrations, and automations can read current locations or listen continuously for movement events—without Home Assistant.", -1),
				Y("button", {
					type: "button",
					onClick: n[12] ||= (t) => Ae(e.snapshotEndpoint, "Snapshot API")
				}, [
					n[78] ||= Y("span", null, "Locations", -1),
					Y("code", null, V(e.snapshotEndpoint), 1),
					n[79] ||= Y("b", null, "Copy", -1)
				]),
				Y("button", {
					type: "button",
					onClick: n[13] ||= (t) => Ae(e.eventsEndpoint, "Live stream")
				}, [
					n[80] ||= Y("span", null, "Live events", -1),
					Y("code", null, V(e.eventsEndpoint), 1),
					n[81] ||= Y("b", null, "Copy", -1)
				])
			]), Y("section", Pz, [Y("header", null, [n[84] ||= Y("div", null, [Y("span", { class: "tv-eyebrow" }, "System health"), Y("h3", null, "Presence diagnostics")], -1), Y("span", null, V(re.value.irk ? "Full identity" : "BLE identity"), 1)]), Y("div", null, [
				Y("article", null, [
					n[85] ||= Y("span", null, "Identity registry", -1),
					Y("strong", null, V(R.value.identity_registry || 0), 1),
					n[86] ||= Y("small", null, "named devices", -1)
				]),
				Y("article", null, [
					n[87] ||= Y("span", null, "Private identities", -1),
					Y("strong", null, V(R.value.irk_identities || 0), 1),
					n[88] ||= Y("small", null, "IRKs stored privately", -1)
				]),
				Y("article", null, [
					n[89] ||= Y("span", null, "iBeacons visible", -1),
					Y("strong", null, V(R.value.ibeacons_visible || 0), 1),
					n[90] ||= Y("small", null, "stable beacon IDs", -1)
				]),
				Y("article", null, [
					n[91] ||= Y("span", null, "Observation buffer", -1),
					Y("strong", null, V(R.value.bounded_observations || 0) + "/" + V(R.value.observation_capacity || 1024), 1),
					n[92] ||= Y("small", null, "bounded server memory", -1)
				]),
				Y("article", null, [
					n[93] ||= Y("span", null, "Movement history", -1),
					Y("strong", null, V(R.value.history_events || 0) + "/" + V(R.value.history_capacity || 2e3), 1),
					n[94] ||= Y("small", null, "persistent bounded events", -1)
				]),
				Y("article", null, [
					n[95] ||= Y("span", null, "Identity cache", -1),
					Y("strong", null, V(R.value.identity_cache || 0) + "/" + V(R.value.identity_cache_capacity || 2048), 1),
					n[96] ||= Y("small", null, "rotating-address lookups", -1)
				])
			])])]),
			ga(kl, {
				open: !!ne.value,
				"backdrop-class": "tv-modal-backdrop tset-modal",
				onClose: n[16] ||= (e) => h.value = ""
			}, {
				default: Sn(() => [ne.value ? (q(), J("section", Fz, [
					Y("header", null, [Y("div", Iz, [Y("span", null, V(he(ne.value.room)), 1), Y("div", null, [
						n[97] ||= Y("small", { class: "tv-eyebrow" }, "Live room devices", -1),
						Y("h2", null, V(ne.value.room), 1),
						Y("p", null, V(ne.value.devices.length) + " located · nearest first", 1)
					])]), Y("button", {
						class: "tv-button",
						type: "button",
						onClick: n[14] ||= (e) => h.value = ""
					}, "Close")]),
					Y("div", Lz, [(q(!0), J(K, null, G(ne.value.devices, (e) => (q(), J("button", {
						key: oe(e),
						type: "button",
						onClick: (t) => we(e, !0)
					}, [
						Y("span", zz, V(ge(e)), 1),
						Y("span", Bz, [Y("strong", null, V(ce(e)), 1), Y("small", null, V(de(e.distance_m)) + " · " + V(ye(e)), 1)]),
						Y("b", null, V(e.strongest_rssi) + " dBm", 1)
					], 8, Rz))), 128))]),
					Y("footer", null, [n[98] ||= Y("span", null, "Select a device to inspect its calibrated location.", -1), Y("button", {
						class: "tv-button primary",
						type: "button",
						onClick: n[15] ||= (e) => h.value = ""
					}, "Done")])
				])) : Z("", !0)]),
				_: 1
			}, 8, ["open"]),
			ga(kl, {
				open: g.value,
				"backdrop-class": "tv-modal-backdrop tset-modal",
				onClose: n[29] ||= (e) => g.value = !1
			}, {
				default: Sn(() => [g.value ? (q(), J("section", Vz, [
					Y("header", null, [Y("div", null, [
						n[99] ||= Y("small", { class: "tv-eyebrow" }, "Tater identity registry", -1),
						Y("h2", Hz, V(y.value.name || "Track BLE device"), 1),
						n[100] ||= Y("p", null, "Names and private identity keys stay on this Tater.", -1)
					]), Y("button", {
						class: "tv-button",
						type: "button",
						onClick: n[17] ||= (e) => g.value = !1
					}, "Close")]),
					Y("div", Uz, [
						Y("div", Wz, [
							Y("label", null, [n[101] ||= Y("span", null, "Display name", -1), W(Y("input", {
								"onUpdate:modelValue": n[18] ||= (e) => y.value.name = e,
								placeholder: "Alice's phone"
							}, null, 512), [[$, y.value.name]])]),
							Y("label", null, [n[102] ||= Y("span", null, "Owner or person", -1), W(Y("input", {
								"onUpdate:modelValue": n[19] ||= (e) => y.value.owner = e,
								placeholder: "Alice"
							}, null, 512), [[$, y.value.owner]])]),
							Y("label", null, [n[104] ||= Y("span", null, "Category", -1), W(Y("select", { "onUpdate:modelValue": n[20] ||= (e) => y.value.category = e }, [...n[103] ||= [
								Y("option", { value: "device" }, "Device", -1),
								Y("option", { value: "phone" }, "Phone", -1),
								Y("option", { value: "watch" }, "Watch", -1),
								Y("option", { value: "person" }, "Person", -1),
								Y("option", { value: "pet" }, "Pet", -1),
								Y("option", { value: "keys" }, "Keys", -1),
								Y("option", { value: "beacon" }, "Beacon", -1)
							]], 512), [[ds, y.value.category]])]),
							Y("label", Gz, [W(Y("input", {
								"onUpdate:modelValue": n[21] ||= (e) => y.value.track = e,
								type: "checkbox"
							}, null, 512), [[cs, y.value.track]]), n[105] ||= Y("span", null, [Y("b", null, "Track home and room state"), Y("small", null, "Record arrivals, departures, and room changes.")], -1)])
						]),
						Y("section", null, [
							n[110] ||= Y("header", null, [Y("div", null, [Y("strong", null, "Stable identity"), Y("small", null, "Use the detected address, iBeacon ID, or a private IRK.")])], -1),
							Y("div", Kz, [
								Y("label", null, [n[106] ||= Y("span", null, "Detected address", -1), W(Y("input", {
									"onUpdate:modelValue": n[22] ||= (e) => y.value.address = e,
									readonly: ""
								}, null, 512), [[$, y.value.address]])]),
								Y("label", null, [n[107] ||= Y("span", null, "iBeacon UUID:major:minor", -1), W(Y("input", {
									"onUpdate:modelValue": n[23] ||= (e) => y.value.ibeaconId = e,
									placeholder: "uuid:1:2"
								}, null, 512), [[$, y.value.ibeaconId]])]),
								Y("label", qz, [
									n[108] ||= Y("span", null, "Identity Resolving Key", -1),
									W(Y("input", {
										"onUpdate:modelValue": n[24] ||= (e) => y.value.irk = e,
										type: "password",
										placeholder: "Paste 32 hex characters or Base64"
									}, null, 512), [[$, y.value.irk]]),
									n[109] ||= Y("small", null, "Leave blank to keep an existing IRK. It is never returned by the API.", -1)
								])
							]),
							y.value.identities.length ? (q(), J("div", Jz, [(q(!0), J(K, null, G(y.value.identities, (e) => (q(), J("span", { key: String(e.type) + String(e.value || e.fingerprint) }, V(e.display || e.type), 1))), 128))])) : Z("", !0)
						]),
						Y("section", null, [n[114] ||= Y("header", null, [Y("div", null, [Y("strong", null, "Optional device calibration"), Y("small", null, "Leave blank to use the whole-home defaults.")])], -1), Y("div", Yz, [
							Y("label", null, [n[111] ||= Y("span", null, "RSSI at one metre", -1), W(Y("input", {
								"onUpdate:modelValue": n[25] ||= (e) => y.value.referencePower = e,
								type: "number",
								min: "-100",
								max: "-20",
								placeholder: "Global default"
							}, null, 512), [[$, y.value.referencePower]])]),
							Y("label", null, [n[112] ||= Y("span", null, "Room radius in metres", -1), W(Y("input", {
								"onUpdate:modelValue": n[26] ||= (e) => y.value.maxRadius = e,
								type: "number",
								min: "1",
								max: "100",
								placeholder: "Global default"
							}, null, 512), [[$, y.value.maxRadius]])]),
							Y("label", null, [n[113] ||= Y("span", null, "Away timeout in seconds", -1), W(Y("input", {
								"onUpdate:modelValue": n[27] ||= (e) => y.value.homeTimeout = e,
								type: "number",
								min: "15",
								max: "3600",
								placeholder: "Global default"
							}, null, 512), [[$, y.value.homeTimeout]])])
						])])
					]),
					Y("footer", null, [
						y.value.configured ? (q(), J("button", {
							key: 0,
							class: "tv-button danger",
							type: "button",
							disabled: s.value,
							onClick: De
						}, "Stop tracking", 8, Xz)) : Z("", !0),
						n[115] ||= Y("span", null, null, -1),
						Y("button", {
							class: "tv-button",
							type: "button",
							onClick: n[28] ||= (e) => g.value = !1
						}, "Cancel"),
						Y("button", {
							class: "tv-button primary",
							type: "button",
							disabled: s.value || !y.value.name.trim(),
							onClick: Ee
						}, V(s.value ? "Saving…" : "Save tracker"), 9, Zz)
					])
				])) : Z("", !0)]),
				_: 1
			}, 8, ["open"])
		]));
	}
}), eB = { class: "tm-stack tvoice-satellites" }, tB = {
	key: 0,
	class: "tv-notice error"
}, nB = { class: "tm-form-card tvoice-pairing-card" }, rB = ["disabled"], iB = {
	key: 1,
	class: "tvoice-section-heading tvoice-device-list-heading"
}, aB = { class: "tvoice-device-head" }, oB = { class: "tvoice-device-identity" }, sB = ["src", "alt"], cB = {
	key: 0,
	class: "tvoice-badges"
}, lB = {
	key: 0,
	class: "tm-detail-list tvoice-summary-list"
}, uB = ["onClick"], dB = {
	key: 1,
	class: "tvoice-volume"
}, fB = [
	"onUpdate:modelValue",
	"min",
	"max",
	"step",
	"disabled",
	"onInput",
	"onChange"
], pB = {
	key: 3,
	class: "tvoice-sensors"
}, mB = { class: "tm-metrics" }, hB = { class: "tm-inline-actions tvoice-item-actions" }, gB = ["disabled", "onClick"], _B = ["disabled", "onClick"], vB = {
	key: 0,
	class: "tvoice-unsaved-dot",
	"aria-label": "Unsaved changes"
}, yB = ["disabled", "onClick"], bB = [
	"disabled",
	"title",
	"onClick"
], xB = ["disabled", "onClick"], SB = {
	key: 5,
	class: "tm-unsaved-label"
}, CB = {
	key: 2,
	class: "tv-notice"
}, wB = {
	key: 0,
	class: "tv-modal tvoice-settings-modal",
	role: "dialog",
	"aria-modal": "true",
	"aria-labelledby": "tvoice-settings-title"
}, TB = { class: "tvoice-settings-modal-head" }, EB = { class: "tvoice-settings-device" }, DB = ["src", "alt"], OB = { id: "tvoice-settings-title" }, kB = { class: "tvoice-settings-overview" }, AB = { key: 0 }, jB = { class: "tvoice-settings-sections" }, MB = {
	key: 0,
	class: "tvoice-settings-block"
}, NB = { key: 1 }, PB = {
	key: 0,
	class: "tv-notice warning"
}, FB = { class: "tvoice-settings-footer" }, IB = ["disabled"], LB = {
	class: "tv-modal tvoice-pairing-modal",
	role: "dialog",
	"aria-modal": "true",
	"aria-labelledby": "tvoice-pairing-title"
}, RB = { key: 0 }, zB = { key: 1 }, BB = { key: 2 }, VB = ["value"], HB = ["href"], UB = ["disabled"], WB = {
	key: 0,
	class: "tv-modal tvoice-info-modal",
	role: "dialog",
	"aria-modal": "true",
	"aria-labelledby": "tvoice-info-title"
}, GB = { class: "tv-eyebrow" }, KB = { id: "tvoice-info-title" }, qB = { class: "tvoice-info-sections" }, JB = /* @__PURE__ */ sr({
	__name: "VoiceSatellites",
	props: {
		payload: {},
		actionEndpoint: {}
	},
	emits: ["refresh", "notify"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ Et({}), a = /* @__PURE__ */ Et({}), o = /* @__PURE__ */ Et({}), s = /* @__PURE__ */ Et({}), c = /* @__PURE__ */ Et({}), l = /* @__PURE__ */ Et({}), u = /* @__PURE__ */ Et({}), d = /* @__PURE__ */ U(""), f = /* @__PURE__ */ U(""), p = /* @__PURE__ */ U(null), m = /* @__PURE__ */ U(!1), h = /* @__PURE__ */ U(0), g = /* @__PURE__ */ U(""), _ = /* @__PURE__ */ U(""), v = null, y = Q(() => YI(n.payload, "satellite")), b = Q(() => y.value.filter((e) => !!e.connected).length), x = Q(() => y.value.find((e) => P(e) === g.value) || null), S = Q(() => y.value.find((e) => P(e) === _.value) || null), C = Q(() => x.value ? ee(x.value) : null), w = Q(() => x.value ? te(x.value) : []), T = Q(() => {
			let e = x.value?.summary_rows;
			return Array.isArray(e) ? e.slice(0, 4) : [];
		}), E = Q(() => n.payload.ui && typeof n.payload.ui == "object" ? n.payload.ui : {}), D = Q(() => E.value.native_pairing && typeof E.value.native_pairing == "object" ? E.value.native_pairing : {}), O = Q(() => {
			let e = n.payload.display_sensors && typeof n.payload.display_sensors == "object" ? n.payload.display_sensors : {};
			return Array.isArray(e.profiles) ? e.profiles : [];
		}), k = Q(() => !!n.payload.display_sensors?.ready), A = Q(() => String(p.value?.state || p.value?.status || "waiting").toLowerCase()), j = Q(() => Math.max(0, Number(p.value?.expires_in_s || 0))), M = Q(() => h.value > 0 ? Math.max(0, Math.min(100, j.value / h.value * 100)) : 0), N = Q(() => A.value === "paired" ? "Satellite connected" : A.value === "expired" ? "Code expired" : "Waiting for satellite");
		function P(e) {
			return String(e.id || "");
		}
		function F(e, t) {
			return [{
				label: e,
				fields: t
			}];
		}
		function I(e) {
			return String(e.run_action || "") === "voice_native_satellite_setup_mode";
		}
		function ee(e) {
			let t = [
				e.id,
				e.selector,
				e.title,
				e.display_target
			].map((e) => String(e || "").trim().toLowerCase()).filter(Boolean);
			return O.value.find((e) => [
				e.selector,
				e.target,
				e.title
			].map((e) => String(e || "").trim().toLowerCase()).some((e) => t.includes(e))) || null;
		}
		function te(e) {
			let t = [], n = {
				label: "Everyday controls",
				description: "Core behavior and local diagnostics for this satellite.",
				fields: []
			};
			return (Array.isArray(e.popup_fields) ? e.popup_fields : []).forEach((e) => {
				if (String(e.type || "").toLowerCase() === "section") {
					n.fields.length && t.push(n), n = {
						label: String(e.label || "Satellite settings"),
						description: String(e.description || ""),
						fields: []
					};
					return;
				}
				n.fields.push(e);
			}), n.fields.length && t.push(n), t;
		}
		function ne(e) {
			let t = [], n = null;
			return (Array.isArray(e.info_fields) ? e.info_fields : []).forEach((e) => {
				if (String(e.type || "") === "section") {
					n = {
						title: String(e.label || "Details"),
						rows: []
					}, t.push(n);
					return;
				}
				n || (n = {
					title: "Details",
					rows: []
				}, t.push(n)), n.rows.push({
					label: e.label,
					value: e.value
				});
			}), t;
		}
		function L(e) {
			let t = Math.max(0, Math.floor(e)), n = Math.floor(t / 60), r = t % 60;
			return `${n}:${String(r).padStart(2, "0")}`;
		}
		function R() {
			let e = /* @__PURE__ */ new Set();
			y.value.forEach((t) => {
				let n = P(t);
				if (!n) return;
				e.add(n), c[n] || (i[n] = XI(Array.isArray(t.fields) ? t.fields : [])), l[n] || (a[n] = XI(Array.isArray(t.popup_fields) ? t.popup_fields : [])), c[`${n}:volume`] || (s[n] = Number(t.volume_control?.value ?? 80));
				let r = ee(t);
				r && !u[n] && (o[n] = XI(Array.isArray(r.fields) ? r.fields : []));
			}), [
				i,
				a,
				o
			].forEach((t) => Object.keys(t).forEach((n) => {
				e.has(n) || delete t[n];
			}));
		}
		function z(e, t, n, r, i) {
			let a = P(n);
			e[a] || (e[a] = {}), e[a][r] = i, t[a] = !0, f.value = "";
		}
		async function re(e, t, i, a, o = !0) {
			if (!e || d.value) return null;
			d.value = i, f.value = "";
			try {
				let i = await $I(n.actionEndpoint, e, t);
				return r("notify", String(i.message || a), "success"), o && r("refresh"), i;
			} catch (e) {
				return f.value = e instanceof Error ? e.message : a, r("notify", f.value, "error"), null;
			} finally {
				d.value = "";
			}
		}
		async function ie(e) {
			let t = P(e);
			await re(String(e.save_action || "voice_satellite_save"), {
				id: t,
				selector: t,
				values: { ...i[t] || {} }
			}, `${t}:save`, "Satellite saved.") && (c[t] = !1);
		}
		async function ae(e) {
			let t = P(e), n = await re(String(e.settings_save_action || "voice_native_satellite_settings_save"), {
				id: t,
				selector: t,
				values: { ...a[t] || {} }
			}, `${t}:settings`, "Satellite settings saved.");
			return n && (l[t] = !1), !!n;
		}
		function oe(e) {
			g.value = P(e);
		}
		function se() {
			g.value = "";
		}
		function ce(e) {
			_.value = P(e);
		}
		function le() {
			_.value = "";
		}
		function ue(e, t) {
			let n = x.value;
			n && z(a, l, n, e, t);
		}
		function de(e, t) {
			let n = x.value;
			n && z(o, u, n, e, t);
		}
		async function fe(e) {
			if (Array.isArray(e.popup_fields) && e.popup_fields.length && !await ae(e)) return;
			let t = ee(e);
			t && k.value && u[P(e)] && !await _e(e, t) || se();
		}
		async function pe(e) {
			let t = P(e), n = Number(e.volume_control?.value ?? 80), r = Math.max(0, Math.min(100, Math.round(Number(s[t] || 0))));
			await re(String(e.volume_control?.action || "voice_native_satellite_settings_save"), {
				id: t,
				selector: t,
				values: { volume_percent: r }
			}, `${t}:volume`, "Satellite volume saved.", !1) ? c[`${t}:volume`] = !1 : s[t] = n;
		}
		async function me(e) {
			await re(String(e.identify_action || "voice_satellite_identify"), {
				id: P(e),
				selector: P(e)
			}, `${P(e)}:identify`, "Identify message played.", !1);
		}
		async function he(e) {
			e.run_confirm && !window.confirm(String(e.run_confirm)) || await re(String(e.run_action || ""), {
				id: P(e),
				selector: P(e)
			}, `${P(e)}:run`, "Satellite action completed.");
		}
		async function ge(e) {
			window.confirm(String(e.remove_confirm || `Forget ${e.title || "this satellite"}?`)) && await re(String(e.remove_action || "voice_satellite_remove"), {
				id: P(e),
				selector: P(e)
			}, `${P(e)}:remove`, "Satellite forgotten.");
		}
		async function _e(e, t) {
			let n = P(e), r = await re("voice_display_sensors_save", {
				target: t.target,
				target_label: t.target_label,
				selector: t.selector || n,
				display_url: t.display_url,
				slots: { ...o[n] || {} }
			}, `${n}:display`, "Display settings saved.");
			return r && (u[n] = !1), !!r;
		}
		async function ve() {
			let e = await re(String(D.value.start_action || "voice_native_satellite_pairing_start"), {}, "pairing", "Pairing code created.", !1);
			e && (p.value = e, h.value = Number(e.expires_in_s || 0), m.value = !0, xe());
		}
		async function ye() {
			if (p.value && A.value === "waiting" && j.value > 0) {
				m.value = !0;
				return;
			}
			await ve();
		}
		async function be() {
			let e = String(p.value?.pairing_id || p.value?.id || "");
			if (e) try {
				let t = await $I(n.actionEndpoint, String(D.value.status_action || "voice_native_satellite_pairing_status"), {
					pairing_id: e,
					id: e
				});
				p.value = t, h.value = Math.max(h.value, Number(t.expires_in_s || 0));
				let i = String(t.state || t.status || "").toLowerCase();
				[
					"paired",
					"connected",
					"complete"
				].includes(i) || t.paired || t.connected ? (H(), r("notify", String(t.message || "Satellite paired."), "success"), r("refresh")) : i === "expired" || t.expired ? H() : xe();
			} catch (e) {
				f.value = e instanceof Error ? e.message : "Pairing status could not be checked.", H();
			}
		}
		function xe() {
			H(), v = window.setTimeout(be, 2e3);
		}
		function H() {
			v !== null && window.clearTimeout(v), v = null;
		}
		return On(() => n.payload, R, {
			deep: !0,
			immediate: !0
		}), kr(H), (t, n) => (q(), J("section", eB, [
			f.value ? (q(), J("div", tB, V(f.value), 1)) : Z("", !0),
			Y("article", nB, [Y("header", null, [n[3] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Tater Native"),
				Y("h3", null, "Add a satellite"),
				Y("p", null, "Pair official Tater hardware with a secure, short-lived setup code.")
			], -1), Y("button", {
				class: "tv-button primary",
				type: "button",
				disabled: !!d.value,
				onClick: ye
			}, V(d.value === "pairing" ? "Creating…" : A.value === "waiting" && j.value > 0 ? "View pairing code" : "Add Satellite"), 9, rB)]), n[4] ||= Y("div", {
				class: "tvoice-pairing-steps",
				"aria-label": "Satellite pairing steps"
			}, [
				Y("span", null, [Y("b", null, "1"), X("Start pairing")]),
				Y("span", null, [Y("b", null, "2"), X("Enter the code on the satellite")]),
				Y("span", null, [Y("b", null, "3"), X("Tater connects it automatically")])
			], -1)]),
			y.value.length ? (q(), J("div", iB, [Y("div", null, [
				n[5] ||= Y("span", { class: "tv-eyebrow" }, "Your satellite network", -1),
				Y("h3", null, V(b.value) + " connected · " + V(y.value.length) + " known", 1),
				n[6] ||= Y("p", null, "Core controls stay on each card; deeper device and display settings open in a focused popup.", -1)
			]), n[7] ||= Y("span", { class: "tvoice-network-state" }, [Y("i"), X("Live")], -1)])) : Z("", !0),
			(q(!0), J(K, null, G(y.value, (e) => (q(), J("article", {
				key: P(e),
				class: "tm-form-card tvoice-satellite-card"
			}, [
				Y("header", aB, [Y("div", oB, [e.hero_image_src ? (q(), J("img", {
					key: 0,
					src: String(e.hero_image_src),
					alt: String(e.hero_image_alt || e.title)
				}, null, 8, sB)) : Z("", !0), Y("div", null, [
					Y("h3", null, V(e.title || P(e)), 1),
					Y("p", null, V(e.subtitle), 1),
					Array.isArray(e.hero_badges) ? (q(), J("div", cB, [(q(!0), J(K, null, G(e.hero_badges, (e) => (q(), J("span", {
						key: String(e.label),
						class: B(`tone-${e.tone || "muted"}`)
					}, V(e.label), 3))), 128))])) : Z("", !0)
				])]), Y("span", { class: B(["tv-live-pill", { warning: !e.connected }]) }, [n[8] ||= Y("i", null, null, -1), X(V(e.connected ? "Connected" : "Offline"), 1)], 2)]),
				Array.isArray(e.summary_rows) && e.summary_rows.length ? (q(), J("dl", lB, [(q(!0), J(K, null, G(e.summary_rows, (e) => (q(), J(K, { key: String(e.label) }, [Y("dt", null, V(e.label), 1), Y("dd", null, V(e.value || "—"), 1)], 64))), 128))])) : Z("", !0),
				(q(!0), J(K, null, G(e.detail_sections || [], (t) => (q(), J("section", {
					key: String(t.title),
					class: "tvoice-device-overview"
				}, [Y("header", null, [Y("span", null, V(t.title), 1), Array.isArray(e.info_fields) && e.info_fields.length ? (q(), J("button", {
					key: 0,
					class: "tm-link-button",
					type: "button",
					onClick: (t) => ce(e)
				}, "View all device info", 8, uB)) : Z("", !0)]), Y("dl", null, [(q(!0), J(K, null, G(t.rows || [], (e) => (q(), J("div", { key: String(e.label) }, [Y("dt", null, V(e.label), 1), Y("dd", null, V(e.value || "—"), 1)]))), 128))])]))), 128)),
				e.volume_control && Object.keys(e.volume_control).length ? (q(), J("label", dB, [Y("span", null, [n[9] ||= Y("strong", null, "Volume", -1), Y("output", null, V(s[P(e)] ?? e.volume_control.value) + "%", 1)]), W(Y("input", {
					"onUpdate:modelValue": (t) => s[P(e)] = t,
					type: "range",
					min: e.volume_control.min ?? 0,
					max: e.volume_control.max ?? 100,
					step: e.volume_control.step ?? 1,
					disabled: !!d.value,
					onInput: (t) => c[`${P(e)}:volume`] = !0,
					onChange: (t) => pe(e)
				}, null, 40, fB), [[
					$,
					s[P(e)],
					void 0,
					{ number: !0 }
				]])])) : Z("", !0),
				Array.isArray(e.fields) && e.fields.length ? (q(), da(Vk, {
					key: 2,
					sections: F("Room and playback", e.fields),
					values: i[P(e)] || {},
					disabled: !!d.value,
					onChange: (t, n) => z(i, c, e, t, n)
				}, null, 8, [
					"sections",
					"values",
					"disabled",
					"onChange"
				])) : Z("", !0),
				Array.isArray(e.sensor_rows) && e.sensor_rows.length ? (q(), J("div", pB, [Y("h4", null, V(e.sensor_title || "Live entities"), 1), Y("div", mB, [(q(!0), J(K, null, G(e.sensor_rows, (e) => (q(), J("article", { key: String(e.key) }, [Y("span", null, V(e.label), 1), Y("strong", null, V(e.value || "—"), 1)]))), 128))])])) : Z("", !0),
				Y("div", hB, [
					e.save_action ? (q(), J("button", {
						key: 0,
						class: "tv-button primary",
						type: "button",
						disabled: !!d.value,
						onClick: (t) => ie(e)
					}, V(d.value === `${P(e)}:save` ? "Saving…" : e.save_label || "Save"), 9, gB)) : Z("", !0),
					Array.isArray(e.popup_fields) && e.popup_fields.length || ee(e) ? (q(), J("button", {
						key: 1,
						class: "tv-button tvoice-settings-trigger",
						type: "button",
						disabled: !!d.value,
						onClick: (t) => oe(e)
					}, [X(V(e.settings_label || "Satellite Settings"), 1), l[P(e)] || u[P(e)] ? (q(), J("span", vB)) : Z("", !0)], 8, _B)) : Z("", !0),
					e.identify_action ? (q(), J("button", {
						key: 2,
						class: "tv-button",
						type: "button",
						disabled: !!d.value || !e.connected,
						onClick: (t) => me(e)
					}, V(e.identify_label || "Identify"), 9, yB)) : Z("", !0),
					e.run_action ? (q(), J("button", {
						key: 3,
						class: B(["tv-button", {
							danger: I(e),
							"tvoice-setup-mode": I(e)
						}]),
						type: "button",
						disabled: !!d.value,
						title: I(e) ? "Unpairs this satellite and restarts it in setup mode" : "",
						onClick: (t) => he(e)
					}, V(d.value === `${P(e)}:run` ? "Working…" : e.run_label || "Run"), 11, bB)) : Z("", !0),
					e.remove_action ? (q(), J("button", {
						key: 4,
						class: "tv-button danger",
						type: "button",
						disabled: !!d.value,
						onClick: (t) => ge(e)
					}, V(e.remove_label || "Forget"), 9, xB)) : Z("", !0),
					c[P(e)] ? (q(), J("span", SB, "Unsaved changes")) : Z("", !0)
				])
			]))), 128)),
			y.value.length ? Z("", !0) : (q(), J("div", CB, V(e.payload.empty_message || "No native satellites are connected yet."), 1)),
			ga(kl, {
				open: !!x.value,
				"backdrop-class": "tv-modal-backdrop tset-modal",
				onClose: se
			}, {
				default: Sn(() => [x.value ? (q(), J("section", wB, [
					Y("header", TB, [Y("div", EB, [x.value.hero_image_src ? (q(), J("img", {
						key: 0,
						src: String(x.value.hero_image_src),
						alt: String(x.value.hero_image_alt || x.value.title)
					}, null, 8, DB)) : Z("", !0), Y("div", null, [
						n[10] ||= Y("span", { class: "tv-eyebrow" }, "Tater satellite controls", -1),
						Y("h2", OB, V(x.value.title || x.value.settings_title || "Satellite Settings"), 1),
						n[11] ||= Y("p", null, "Fine-tune device behavior, lighting, and display sensors in one place.", -1)
					])]), Y("button", {
						class: "tv-button tvoice-settings-close",
						type: "button",
						onClick: se
					}, "Close")]),
					Y("div", kB, [Y("span", { class: B(["tv-live-pill", { warning: !x.value.connected }]) }, [n[12] ||= Y("i", null, null, -1), X(V(x.value.connected ? "Connected" : "Offline"), 1)], 2), T.value.length ? (q(), J("dl", AB, [(q(!0), J(K, null, G(T.value, (e) => (q(), J("div", { key: String(e.label) }, [Y("dt", null, V(e.label), 1), Y("dd", null, V(e.value || "—"), 1)]))), 128))])) : Z("", !0)]),
					Y("div", jB, [w.value.length ? (q(), J("section", MB, [n[13] ||= Y("header", null, [Y("span", { class: "tvoice-section-mark" }, "SAT"), Y("div", null, [Y("h3", null, "Satellite behavior"), Y("p", null, "Each group applies only to this device. LED previews update as you choose an animation.")])], -1), ga(Vk, {
						class: "tvoice-settings-groups",
						sections: w.value,
						values: a[P(x.value)] || {},
						disabled: !!d.value,
						onChange: ue
					}, null, 8, [
						"sections",
						"values",
						"disabled"
					])])) : Z("", !0), C.value ? (q(), J("section", NB, [
						Y("header", null, [n[15] ||= Y("span", { class: "tvoice-section-mark" }, "DSP", -1), Y("div", null, [n[14] ||= Y("h3", null, "Display sensors", -1), Y("p", null, V(C.value.detail || "Choose which Environment Core readings this display shows."), 1)])]),
						ga(Vk, {
							sections: F("Sensor slots", Array.isArray(C.value.fields) ? C.value.fields : []),
							values: o[P(x.value)] || {},
							disabled: !!d.value || !k.value,
							onChange: de
						}, null, 8, [
							"sections",
							"values",
							"disabled"
						]),
						k.value ? Z("", !0) : (q(), J("div", PB, V(e.payload.display_sensors?.message || "Environment Core readings are not available yet."), 1))
					])) : Z("", !0)]),
					Y("footer", FB, [Y("span", { class: B(["tvoice-settings-sync", { dirty: l[P(x.value)] || u[P(x.value)] }]) }, [n[16] ||= Y("i", null, null, -1), X(V(l[P(x.value)] || u[P(x.value)] ? "Unsaved changes" : "Satellite settings are synchronized"), 1)], 2), Y("button", {
						class: "tv-button primary",
						type: "button",
						disabled: !!d.value,
						onClick: n[0] ||= (e) => fe(x.value)
					}, V(d.value ? "Saving…" : "Save Satellite Settings"), 9, IB)])
				])) : Z("", !0)]),
				_: 1
			}, 8, ["open"]),
			ga(kl, {
				open: m.value,
				"backdrop-class": "tv-modal-backdrop tset-modal",
				onClose: n[2] ||= (e) => m.value = !1
			}, {
				default: Sn(() => [Y("section", LB, [
					Y("header", null, [n[17] ||= Y("div", null, [
						Y("span", { class: "tv-eyebrow" }, "Secure satellite pairing"),
						Y("h2", { id: "tvoice-pairing-title" }, "Connect a Tater satellite"),
						Y("p", null, "The code works once and expires automatically.")
					], -1), Y("button", {
						class: "tv-button",
						type: "button",
						onClick: n[1] ||= (e) => m.value = !1
					}, "Close")]),
					Y("div", { class: B(["tvoice-pairing-code", `state-${A.value}`]) }, [
						Y("span", null, V(N.value), 1),
						Y("strong", null, V(p.value?.pairing_code || p.value?.display_code || p.value?.code || "—"), 1),
						A.value === "waiting" ? (q(), J("small", RB, "Expires in " + V(L(j.value)), 1)) : A.value === "paired" ? (q(), J("small", zB, "The satellite is paired and ready to appear in this list.")) : (q(), J("small", BB, "Create a fresh code to continue.")),
						A.value === "waiting" ? (q(), J("progress", {
							key: 3,
							value: M.value,
							max: "100"
						}, null, 8, VB)) : Z("", !0)
					], 2),
					n[18] ||= Y("ol", { class: "tvoice-pairing-guide" }, [
						Y("li", null, [Y("b", null, "Open setup on the satellite."), Y("span", null, "Connect the satellite to power and continue until it asks for a Tater pairing code.")]),
						Y("li", null, [Y("b", null, "Enter the six-digit code."), Y("span", null, "Keep this window open while the satellite connects.")]),
						Y("li", null, [Y("b", null, "Wait for confirmation."), Y("span", null, "This dialog updates automatically when Tater recognizes the device.")])
					], -1),
					p.value?.pairing_url || p.value?.url ? (q(), J("a", {
						key: 0,
						class: "tv-button",
						href: String(p.value.pairing_url || p.value.url),
						target: "_blank",
						rel: "noreferrer"
					}, "Open setup page ↗", 8, HB)) : Z("", !0),
					Y("footer", null, [Y("span", null, V(A.value === "waiting" ? "Checking for your satellite…" : N.value), 1), Y("button", {
						class: "tv-button primary",
						type: "button",
						disabled: !!d.value,
						onClick: ve
					}, V(d.value === "pairing" ? "Creating…" : "Create new code"), 9, UB)])
				])]),
				_: 1
			}, 8, ["open"]),
			ga(kl, {
				open: !!S.value,
				"backdrop-class": "tv-modal-backdrop tset-modal",
				onClose: le
			}, {
				default: Sn(() => [S.value ? (q(), J("section", WB, [Y("header", null, [Y("div", null, [
					Y("span", GB, V(S.value.title || "Satellite"), 1),
					Y("h2", KB, V(S.value.info_title || "Device information"), 1),
					n[19] ||= Y("p", null, "Live identity, connection, diagnostics, and firmware details.", -1)
				]), Y("button", {
					class: "tv-button",
					type: "button",
					onClick: le
				}, "Close")]), Y("div", qB, [(q(!0), J(K, null, G(ne(S.value), (e) => (q(), J("section", { key: String(e.title) }, [Y("h3", null, V(e.title), 1), Y("dl", null, [(q(!0), J(K, null, G(e.rows || [], (e) => (q(), J("div", { key: String(e.label) }, [Y("dt", null, V(e.label), 1), Y("dd", null, V(e.value || "—"), 1)]))), 128))])]))), 128))])])) : Z("", !0)]),
				_: 1
			}, 8, ["open"])
		]));
	}
}), YB = { class: "tm-stack tvoice-stats" }, XB = {
	key: 0,
	class: "tv-notice error"
}, ZB = { class: "tm-form-card tvoice-subhero tvoice-stats-hero" }, QB = { class: "tvoice-subhero-metrics" }, $B = { class: "tm-card-grid tvoice-stat-sections" }, eV = { class: "tvoice-card-identity" }, tV = { class: "tvoice-card-mark" }, nV = { class: "tm-metrics tvoice-metric-grid" }, rV = { class: "tm-table-wrap" }, iV = { key: 0 }, aV = ["colspan"], oV = {
	key: 1,
	class: "tv-notice"
}, sV = {
	key: 2,
	class: "tset-save-bar tvoice-save-bar"
}, cV = ["disabled"], lV = /* @__PURE__ */ sr({
	__name: "VoiceStats",
	props: {
		payload: {},
		actionEndpoint: {}
	},
	emits: ["refresh", "notify"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ U(!1), a = /* @__PURE__ */ U(""), o = Q(() => Array.isArray(n.payload.stats_sections) ? n.payload.stats_sections : []), s = Q(() => Array.isArray(n.payload.stats_tables) ? n.payload.stats_tables : []), c = Q(() => n.payload.stats_controls && typeof n.payload.stats_controls == "object" ? n.payload.stats_controls : {}), l = Q(() => o.value.reduce((e, t) => e + (Array.isArray(t.metrics) ? t.metrics.length : 0), 0)), u = Q(() => s.value.reduce((e, t) => e + (Array.isArray(t.rows) ? t.rows.length : 0), 0));
		async function d() {
			if (!(i.value || !c.value.reset_action) && window.confirm(String(c.value.reset_confirm || "Reset all stored voice statistics?"))) {
				i.value = !0, a.value = "";
				try {
					let e = await $I(n.actionEndpoint, String(c.value.reset_action || "voice_statistics_reset"), { id: c.value.id });
					r("notify", String(e.message || "Voice statistics reset."), "success"), r("refresh");
				} catch (e) {
					a.value = e instanceof Error ? e.message : "Voice statistics could not be reset.", r("notify", a.value, "error");
				} finally {
					i.value = !1;
				}
			}
		}
		return (e, t) => (q(), J("section", YB, [
			a.value ? (q(), J("div", XB, V(a.value), 1)) : Z("", !0),
			Y("section", ZB, [t[2] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Live voice health"),
				Y("h3", null, "See how every conversation performs"),
				Y("p", null, "Quality, latency, backend fallbacks, and device outcomes update automatically while this tab is open.")
			], -1), Y("div", QB, [Y("span", null, [Y("b", null, V(l.value), 1), t[0] ||= X("Live signals", -1)]), Y("span", null, [Y("b", null, V(u.value), 1), t[1] ||= X("Device rows", -1)])])]),
			Y("div", $B, [(q(!0), J(K, null, G(o.value, (e, n) => (q(), J("article", {
				key: String(e.title),
				class: "tm-form-card tvoice-stat-card"
			}, [Y("header", null, [Y("div", eV, [Y("span", tV, V(n + 1), 1), Y("div", null, [t[3] ||= Y("span", { class: "tv-eyebrow" }, "Live measurements", -1), Y("h3", null, V(e.title), 1)])]), Y("span", null, V((e.metrics || []).length) + " signals", 1)]), Y("div", nV, [(q(!0), J(K, null, G(e.metrics || [], (e) => (q(), J("article", { key: String(e.label) }, [Y("span", null, V(e.label), 1), Y("strong", null, V(e.value), 1)]))), 128))])]))), 128))]),
			(q(!0), J(K, null, G(s.value, (e) => (q(), J("article", {
				key: String(e.title),
				class: "tm-form-card tvoice-stats-table"
			}, [Y("header", null, [Y("div", null, [t[4] ||= Y("span", { class: "tv-eyebrow" }, "Per-satellite detail", -1), Y("h3", null, V(e.title), 1)]), Y("span", null, V((e.rows || []).length) + " rows", 1)]), Y("div", rV, [Y("table", null, [Y("thead", null, [Y("tr", null, [(q(!0), J(K, null, G(e.columns || [], (e) => (q(), J("th", { key: String(e.key) }, V(e.label || e.key), 1))), 128))])]), Y("tbody", null, [(q(!0), J(K, null, G(e.rows || [], (t, n) => (q(), J("tr", { key: n }, [(q(!0), J(K, null, G(e.columns || [], (e) => (q(), J("td", { key: String(e.key) }, V(t[String(e.key)] ?? "—"), 1))), 128))]))), 128)), (e.rows || []).length ? Z("", !0) : (q(), J("tr", iV, [Y("td", { colspan: Math.max(1, (e.columns || []).length) }, V(e.empty_message || "No measurements yet."), 9, aV)]))])])])]))), 128)),
			!o.value.length && !s.value.length ? (q(), J("div", oV, "No voice statistics are available yet.")) : Z("", !0),
			c.value.reset_action ? (q(), J("footer", sV, [Y("div", null, [t[5] ||= Y("strong", null, "Statistics update automatically", -1), Y("span", null, V(c.value.description), 1)]), Y("button", {
				class: "tv-button danger",
				type: "button",
				disabled: i.value,
				onClick: d
			}, V(i.value ? "Resetting…" : c.value.reset_label || "Reset Voice Statistics"), 9, cV)])) : Z("", !0)
		]));
	}
}), uV = { class: "tm-stack tvoice-stereo" }, dV = {
	key: 0,
	class: "tv-notice error"
}, fV = { class: "tm-form-card tvoice-subhero tvoice-stereo-hero" }, pV = { class: "tvoice-subhero-metrics" }, mV = { class: "tvoice-pair-head" }, hV = { class: "tvoice-card-identity" }, gV = { class: "tvoice-card-mark" }, _V = { class: "tv-eyebrow" }, vV = {
	key: 0,
	class: "tm-detail-list tvoice-pair-summary"
}, yV = { class: "tm-inline-actions tvoice-item-actions" }, bV = ["disabled", "onClick"], xV = ["disabled", "onClick"], SV = {
	key: 1,
	class: "tm-unsaved-label"
}, CV = {
	key: 1,
	class: "tv-notice"
}, wV = /* @__PURE__ */ sr({
	__name: "VoiceStereo",
	props: {
		payload: {},
		actionEndpoint: {}
	},
	emits: ["refresh", "notify"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ Et({}), a = /* @__PURE__ */ Et({}), o = /* @__PURE__ */ U(""), s = /* @__PURE__ */ U(""), c = Q(() => YI(n.payload).filter((e) => ["stereo_pair", "stereo_pair_create"].includes(String(e.group || "")))), l = Q(() => c.value.filter((e) => String(e.group || "") === "stereo_pair")), u = Q(() => l.value.filter((e) => !!e.connected).length);
		function d(e) {
			return String(e.id || e.title || "pair");
		}
		function f(e) {
			return [{
				label: e.group === "stereo_pair_create" ? "New pair" : "Pair configuration",
				fields: Array.isArray(e.fields) ? e.fields : []
			}];
		}
		function p() {
			let e = /* @__PURE__ */ new Set();
			c.value.forEach((t) => {
				let n = d(t);
				e.add(n), a[n] || (i[n] = XI(Array.isArray(t.fields) ? t.fields : []));
			}), Object.keys(i).forEach((t) => {
				e.has(t) || (delete i[t], delete a[t]);
			});
		}
		function m(e, t, n) {
			let r = d(e);
			i[r] || (i[r] = {}), i[r][t] = n, a[r] = !0, s.value = "";
		}
		async function h(e) {
			let t = d(e);
			if (!o.value) {
				o.value = t, s.value = "";
				try {
					let o = e.group === "stereo_pair_create" ? "" : String(e.id || ""), s = await $I(n.actionEndpoint, String(e.save_action || "voice_stereo_pair_save"), {
						id: o,
						selector: o,
						values: { ...i[t] || {} }
					});
					a[t] = !1, r("notify", String(s.message || (o ? "Stereo pair saved." : "Stereo pair created.")), "success"), r("refresh");
				} catch (e) {
					s.value = e instanceof Error ? e.message : "Stereo pair could not be saved.", r("notify", s.value, "error");
				} finally {
					o.value = "";
				}
			}
		}
		async function g(e) {
			if (!e.remove_action || !window.confirm(String(e.remove_confirm || `Delete ${e.title || "this stereo pair"}?`))) return;
			let t = d(e);
			o.value = t;
			try {
				let t = await $I(n.actionEndpoint, String(e.remove_action), {
					id: e.id,
					selector: e.id
				});
				r("notify", String(t.message || "Stereo pair deleted."), "success"), r("refresh");
			} catch (e) {
				s.value = e instanceof Error ? e.message : "Stereo pair could not be deleted.", r("notify", s.value, "error");
			} finally {
				o.value = "";
			}
		}
		return On(() => n.payload, p, {
			deep: !0,
			immediate: !0
		}), (e, t) => (q(), J("section", uV, [
			s.value ? (q(), J("div", dV, V(s.value), 1)) : Z("", !0),
			Y("section", fV, [t[2] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Room-filling playback"),
				Y("h3", null, "Build synchronized stereo rooms"),
				Y("p", null, "Pair two compatible satellites as left and right speakers while keeping each device available for normal voice turns.")
			], -1), Y("div", pV, [Y("span", null, [Y("b", null, V(l.value.length), 1), t[0] ||= X("Saved pairs", -1)]), Y("span", null, [Y("b", null, V(u.value), 1), t[1] ||= X("Ready now", -1)])])]),
			(q(!0), J(K, null, G(c.value, (e) => (q(), J("article", {
				key: d(e),
				class: B(["tm-form-card tvoice-pair-card", { "is-new": e.group === "stereo_pair_create" }])
			}, [
				Y("header", mV, [Y("div", hV, [Y("span", gV, V(e.group === "stereo_pair_create" ? "+" : "2X"), 1), Y("div", null, [
					Y("span", _V, V(e.group === "stereo_pair_create" ? "Create a room" : "Stereo destination"), 1),
					Y("h3", null, V(e.title || "Stereo Pair"), 1),
					Y("p", null, [X(V(e.subtitle), 1), e.detail ? (q(), J(K, { key: 0 }, [X(" · " + V(e.detail), 1)], 64)) : Z("", !0)])
				])]), Y("span", { class: B(["tv-live-pill", { warning: e.group !== "stereo_pair_create" && !e.connected }]) }, [t[3] ||= Y("i", null, null, -1), X(V(e.group === "stereo_pair_create" ? "New pair" : e.connected ? "Ready" : "Unavailable"), 1)], 2)]),
				Array.isArray(e.summary_rows) && e.summary_rows.length ? (q(), J("dl", vV, [(q(!0), J(K, null, G(e.summary_rows, (e) => (q(), J(K, { key: String(e.label) }, [Y("dt", null, V(e.label), 1), Y("dd", null, V(e.value || "—"), 1)], 64))), 128))])) : Z("", !0),
				ga(Vk, {
					sections: f(e),
					values: i[d(e)] || {},
					disabled: !!o.value,
					onChange: (t, n) => m(e, t, n)
				}, null, 8, [
					"sections",
					"values",
					"disabled",
					"onChange"
				]),
				Y("div", yV, [
					Y("button", {
						class: "tv-button primary",
						type: "button",
						disabled: !!o.value,
						onClick: (t) => h(e)
					}, V(o.value === d(e) ? "Saving…" : e.save_label || "Save Pair"), 9, bV),
					e.remove_action ? (q(), J("button", {
						key: 0,
						class: "tv-button danger",
						type: "button",
						disabled: !!o.value,
						onClick: (t) => g(e)
					}, V(e.remove_label || "Delete Pair"), 9, xV)) : Z("", !0),
					a[d(e)] ? (q(), J("span", SV, "Unsaved changes")) : Z("", !0)
				])
			], 2))), 128)),
			c.value.length ? Z("", !0) : (q(), J("div", CV, "No stereo-pair configuration is available."))
		]));
	}
}), TV = { class: "tset-resource tvoice-resource" }, EV = { class: "tv-panel tvoice-hero" }, DV = {
	class: "tv-tabs tvoice-tabs",
	"aria-label": "Satellite settings sections"
}, OV = ["onClick"], kV = { class: "tvoice-tab-mark" }, AV = { class: "tset-context" }, jV = {
	key: 0,
	class: "tv-notice error",
	"aria-live": "polite"
}, MV = {
	key: 1,
	class: "tv-notice"
}, NV = {
	key: 2,
	class: "tm-metrics tvoice-metrics"
}, PV = /* @__PURE__ */ sr({
	__name: "VoiceSettings",
	props: {
		runtimeEndpoint: {},
		actionEndpoint: {},
		presenceEndpoint: {},
		presenceEventsEndpoint: {},
		initialTab: {},
		onTabChange: { type: Function }
	},
	emits: ["notify"],
	setup(e, { expose: t, emit: n }) {
		let r = e, i = n, a = [
			{
				id: "satellites",
				label: "Satellites",
				short: "Pair & control",
				icon: "SAT",
				description: "Pair devices, manage rooms, volume, playback, and live satellite settings."
			},
			{
				id: "firmware",
				label: "Firmware",
				short: "Update & recover",
				icon: "FW",
				description: "Install official firmware over OTA or USB and follow progress live."
			},
			{
				id: "stereo",
				label: "Stereo Pairs",
				short: "Left & right",
				icon: "2X",
				description: "Create synchronized left and right playback destinations."
			},
			{
				id: "airplay",
				label: "AirPlay Input",
				short: "Stream to sats",
				icon: "AP",
				description: "Send audio from Apple devices into Tater satellites and speaker groups."
			},
			{
				id: "presence",
				label: "Presence",
				short: "Live room map",
				icon: "BLE",
				description: "Watch nearby Bluetooth devices move between the rooms covered by native satellites."
			},
			{
				id: "stats",
				label: "Stats",
				short: "Quality & latency",
				icon: "LIVE",
				description: "Live voice quality, latency, fallbacks, and per-satellite outcomes."
			},
			{
				id: "platform",
				label: "Settings",
				short: "Shared behavior",
				icon: "SET",
				description: "Shared satellite behavior and native voice-pipeline settings."
			}
		], o = new Set(a.map((e) => e.id)), s = (e) => {
			let t = String(e || "").trim().toLowerCase();
			return o.has(t) ? t : "satellites";
		}, c = /* @__PURE__ */ U(s(r.initialTab)), l = /* @__PURE__ */ U({}), u = /* @__PURE__ */ U(!1), d = /* @__PURE__ */ U(!1), f = /* @__PURE__ */ U(""), p = null, m = 0, h = Q(() => a.find((e) => e.id === c.value) || a[0]), g = Q(() => Array.isArray(l.value.header_stats) ? l.value.header_stats : []), _ = Q(() => ["platform", "presence"].includes(c.value) ? "satellites" : c.value);
		async function v(e = !1) {
			let t = ++m;
			e ? d.value = !0 : u.value = !0, e || (f.value = "");
			try {
				let e = await As(`${r.runtimeEndpoint}?panel=${encodeURIComponent(_.value)}`);
				if (t !== m) return;
				l.value = e.payload && typeof e.payload == "object" ? e.payload : e;
			} catch (e) {
				if (t !== m) return;
				f.value = e instanceof Error ? e.message : "Voice runtime could not be loaded.";
			} finally {
				t === m && (u.value = !1, d.value = !1);
			}
		}
		function y(e) {
			c.value = s(e), f.value = "", l.value = {}, r.onTabChange?.(c.value), v();
		}
		function b(e, t = "success") {
			i("notify", e, t);
		}
		return t({ select: y }), On(() => r.initialTab, (e) => {
			let t = s(e);
			t !== c.value && y(t);
		}), Er(() => {
			r.onTabChange?.(c.value), v(), p = window.setInterval(() => {
				document.visibilityState === "visible" && !u.value && v(!0);
			}, 1e4);
		}), kr(() => {
			p !== null && window.clearInterval(p), m += 1;
		}), (t, n) => (q(), J("section", TV, [
			Y("section", EV, [n[7] ||= Y("div", { class: "tvoice-hero-copy" }, [
				Y("span", { class: "tv-eyebrow" }, "Tater satellite control center"),
				Y("h2", null, "Your voice hardware, in one friendly workspace"),
				Y("p", null, "Pair satellites, keep firmware current, build stereo rooms, and tune Tater’s live voice pipeline without leaving Settings.")
			], -1), Y("div", { class: B(["tvoice-hero-signal", { warning: f.value }]) }, [n[6] ||= Y("span", { class: "tvoice-signal-orbit" }, [
				Y("i"),
				Y("i"),
				Y("i")
			], -1), Y("div", null, [Y("strong", null, V(d.value ? "Refreshing devices" : `${h.value.label} ready`), 1), Y("small", null, V(f.value ? "Runtime needs attention" : "Live satellite state"), 1)])], 2)]),
			Y("nav", DV, [(q(), J(K, null, G(a, (e) => Y("button", {
				key: e.id,
				type: "button",
				class: B({ active: c.value === e.id }),
				onClick: (t) => y(e.id)
			}, [Y("span", kV, V(e.icon), 1), Y("span", null, [Y("b", null, V(e.label), 1), Y("small", null, V(e.short), 1)])], 10, OV)), 64))]),
			Y("div", AV, [Y("span", null, V(h.value.label), 1), Y("p", null, V(h.value.description), 1)]),
			f.value ? (q(), J("div", jV, V(f.value), 1)) : Z("", !0),
			u.value ? (q(), J("div", MV, "Loading " + V(h.value.label) + "…", 1)) : Z("", !0),
			g.value.length && c.value !== "stats" ? (q(), J("div", NV, [(q(!0), J(K, null, G(g.value, (e) => (q(), J("article", { key: String(e.label) }, [Y("span", null, V(e.label), 1), Y("strong", null, V(e.value), 1)]))), 128))])) : Z("", !0),
			c.value === "satellites" && !u.value ? (q(), da(JB, {
				key: 3,
				payload: l.value,
				"action-endpoint": e.actionEndpoint,
				onRefresh: n[0] ||= (e) => v(!0),
				onNotify: b
			}, null, 8, ["payload", "action-endpoint"])) : c.value === "presence" && !u.value ? (q(), da($z, {
				key: 4,
				"snapshot-endpoint": e.presenceEndpoint,
				"events-endpoint": e.presenceEventsEndpoint,
				onNotify: b
			}, null, 8, ["snapshot-endpoint", "events-endpoint"])) : c.value === "firmware" && !u.value ? (q(), da(GL, {
				key: 5,
				payload: l.value,
				"action-endpoint": e.actionEndpoint,
				onRefresh: n[1] ||= (e) => v(!0),
				onNotify: b
			}, null, 8, ["payload", "action-endpoint"])) : c.value === "stereo" && !u.value ? (q(), da(wV, {
				key: 6,
				payload: l.value,
				"action-endpoint": e.actionEndpoint,
				onRefresh: n[2] ||= (e) => v(!0),
				onNotify: b
			}, null, 8, ["payload", "action-endpoint"])) : c.value === "airplay" && !u.value ? (q(), da(gR, {
				key: 7,
				payload: l.value,
				"action-endpoint": e.actionEndpoint,
				onRefresh: n[3] ||= (e) => v(!0),
				onNotify: b
			}, null, 8, ["payload", "action-endpoint"])) : c.value === "stats" && !u.value ? (q(), da(lV, {
				key: 8,
				payload: l.value,
				"action-endpoint": e.actionEndpoint,
				onRefresh: n[4] ||= (e) => v(!0),
				onNotify: b
			}, null, 8, ["payload", "action-endpoint"])) : c.value === "platform" && !u.value ? (q(), da(FR, {
				key: 9,
				payload: l.value,
				"action-endpoint": e.actionEndpoint,
				onRefresh: n[5] ||= (e) => v(!0),
				onNotify: b
			}, null, 8, ["payload", "action-endpoint"])) : Z("", !0)
		]));
	}
}), FV = { class: "tater-vue-surface tset-settings" }, IV = { class: "tv-page-heading" }, LV = { class: "tv-heading-actions" }, RV = { class: "tv-metrics tset-metrics" }, zV = {
	class: "tv-tabs tset-tabs",
	"aria-label": "Settings sections"
}, BV = ["data-settings-vue-tab", "onClick"], VV = {
	class: "tset-context",
	"aria-live": "polite"
}, HV = /* @__PURE__ */ sr({
	__name: "SettingsApp",
	props: {
		state: {},
		options: {}
	},
	setup(e, { expose: t }) {
		let n = e, r = [
			{
				id: "general",
				label: "General",
				description: "Appearance, identity, login, avatars, and everyday WebUI behavior."
			},
			{
				id: "people",
				label: "People",
				description: "Recognized people, user records, and identity management."
			},
			{
				id: "models",
				label: "Models",
				description: "LLM, vision, speech, wake word, speaker, and emotion models."
			},
			{
				id: "hydra",
				label: "Hydra",
				description: "Model routing, role assignments, fallback behavior, and live metrics."
			},
			{
				id: "esphome",
				label: "Satellites",
				description: "Pair satellites, install firmware, build stereo pairs, and tune the live voice runtime."
			},
			{
				id: "redis",
				label: "Redis",
				description: "Data service connection, encryption, recovery, and storage health."
			},
			{
				id: "spudhub",
				label: "Spud Link",
				description: "Hub, Spudlet, and Little Spud pairing and linked-node management."
			},
			{
				id: "misc",
				label: "Misc",
				description: "Chat history, attachments, uploads, and other supporting behavior."
			},
			{
				id: "advanced",
				label: "Advanced",
				description: "Admin-gated tools, limits, security controls, and expert options."
			},
			{
				id: "system",
				label: "System Tasks",
				description: "Live background snapshots, scheduled maintenance, process health, and run history."
			},
			{
				id: "logs",
				label: "Logs",
				description: "Live application logs with filters, pause, copy, and tail controls."
			}
		], i = new Set(r.map((e) => e.id)), a = (e) => {
			let t = String(e || "").trim().toLowerCase();
			return i.has(t) ? t : "general";
		}, o = /* @__PURE__ */ U(a(n.options.initialTab)), s = /* @__PURE__ */ U(null), c = Q(() => r.find((e) => e.id === o.value) || r[0]), l = Q(() => n.state.summary || {});
		async function u(e, t = !1, r = "") {
			let i = a(e);
			o.value = i, t && n.options.onTabChange?.(i), i === "esphome" && r && (await un(), await s.value?.select?.(r));
		}
		function d(e) {
			n.state.general = e, n.options.onGeneralChange?.(e);
		}
		function f(e) {
			n.state.hydra = e, n.options.onHydraChange?.(e);
		}
		function p(e) {
			n.state.misc = e, n.options.onMiscChange?.(e);
		}
		function m(e) {
			n.state.people = e, n.options.onPeopleChange?.(e);
		}
		function h(e) {
			n.state.spudLink = e, n.options.onSpudLinkChange?.(e);
		}
		function g(e) {
			n.state.models = e, n.options.onModelsChange?.(e);
		}
		function _(e) {
			n.state.summary = {
				...n.state.summary,
				redisConnected: !!e.connected
			}, n.options.onRedisStatusChange?.(e);
		}
		function v(e) {
			n.state.advanced = e, n.state.summary = {
				...n.state.summary,
				adminGateCount: Array.isArray(e.admin_only_plugins) ? e.admin_only_plugins.length : 0
			}, n.options.onAdvancedChange?.(e);
		}
		function y(e, t = "success") {
			n.options.onToast?.(e, t);
		}
		return t({ select: (e, t = "") => u(e, !1, t) }), (t, n) => (q(), J("div", FV, [
			Y("header", IV, [n[5] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Tater configuration"),
				Y("h1", null, "Settings"),
				Y("p", null, "Configure identity, intelligence, voice, storage, security, and diagnostics from one workspace.")
			], -1), Y("div", LV, [Y("span", { class: B(["tv-live-pill", { warning: !l.value.redisConnected }]) }, [n[4] ||= Y("i", null, null, -1), X(V(l.value.redisConnected ? "Services connected" : "Redis needs attention"), 1)], 2)])]),
			Y("div", RV, [
				Y("div", null, [n[6] ||= Y("span", null, "Redis", -1), Y("strong", null, V(l.value.redisConnected ? "Connected" : "Setup needed"), 1)]),
				Y("div", null, [n[7] ||= Y("span", null, "Admin gated", -1), Y("strong", null, V(Number(l.value.adminGateCount || 0)), 1)]),
				Y("div", null, [n[8] ||= Y("span", null, "Integrations", -1), Y("strong", null, V(Number(l.value.integrationCount || 0)), 1)])
			]),
			Y("nav", zV, [(q(), J(K, null, G(r, (e) => Y("button", {
				key: e.id,
				type: "button",
				class: B({ active: o.value === e.id }),
				"data-settings-vue-tab": e.id,
				onClick: (t) => u(e.id, !0)
			}, V(e.label), 11, BV)), 64))]),
			Y("div", VV, [Y("span", null, V(c.value.label), 1), Y("p", null, V(c.value.description), 1)]),
			o.value === "general" ? (q(), da(Qx, {
				key: 0,
				settings: e.state.general,
				endpoint: e.options.endpoints.general,
				"on-theme-preview": e.options.onThemePreview,
				onSaved: d,
				onNotify: y
			}, null, 8, [
				"settings",
				"endpoint",
				"on-theme-preview"
			])) : o.value === "hydra" ? (q(), da(YS, {
				key: 1,
				settings: e.state.hydra,
				endpoint: e.options.endpoints.hydra,
				"metrics-endpoint": e.options.endpoints.hydraMetrics,
				"data-endpoint": e.options.endpoints.hydraData,
				"clear-data-endpoint": e.options.endpoints.hydraDataClear,
				onSaved: f,
				onNotify: y
			}, null, 8, [
				"settings",
				"endpoint",
				"metrics-endpoint",
				"data-endpoint",
				"clear-data-endpoint"
			])) : o.value === "models" ? (q(), da(MM, {
				key: 2,
				settings: e.state.models,
				endpoints: e.options.endpoints,
				"initial-tab": e.options.initialModelsTab,
				"on-tab-change": e.options.onModelsTabChange,
				onChanged: g,
				onNotify: y
			}, null, 8, [
				"settings",
				"endpoints",
				"initial-tab",
				"on-tab-change"
			])) : o.value === "people" ? (q(), da(OP, {
				key: 3,
				payload: e.state.people,
				endpoint: e.options.endpoints.people,
				"action-endpoint": e.options.endpoints.peopleAction,
				"initial-tab": e.options.initialPeopleTab,
				"initial-sort": e.options.initialPeopleSort,
				onChanged: m,
				onTabChange: n[0] ||= (t) => e.options.onPeopleTabChange?.(t),
				onSortChange: n[1] ||= (t) => e.options.onPeopleSortChange?.(t),
				onNotify: y
			}, null, 8, [
				"payload",
				"endpoint",
				"action-endpoint",
				"initial-tab",
				"initial-sort"
			])) : o.value === "spudhub" ? (q(), da(_I, {
				key: 4,
				settings: e.state.spudLink,
				endpoint: e.options.endpoints.spudLink,
				"status-endpoint": e.options.endpoints.spudLinkStatus,
				"pairing-code-endpoint": e.options.endpoints.spudLinkPairingCode,
				"connect-endpoint": e.options.endpoints.spudLinkConnect,
				"revoke-endpoint": e.options.endpoints.spudLinkRevoke,
				"llm-api-url": e.options.publicEndpoints.spudLinkLlm,
				"models-api-url": e.options.publicEndpoints.spudLinkModels,
				"pair-api-url": e.options.publicEndpoints.spudLinkPair,
				"initial-tab": e.options.initialSpudLinkTab,
				onChanged: h,
				onTabChange: n[2] ||= (t) => e.options.onSpudLinkTabChange?.(t),
				onNotify: y
			}, null, 8, [
				"settings",
				"endpoint",
				"status-endpoint",
				"pairing-code-endpoint",
				"connect-endpoint",
				"revoke-endpoint",
				"llm-api-url",
				"models-api-url",
				"pair-api-url",
				"initial-tab"
			])) : o.value === "esphome" ? (q(), da(PV, {
				key: 5,
				ref_key: "voicePanel",
				ref: s,
				"runtime-endpoint": e.options.endpoints.voiceRuntime,
				"action-endpoint": e.options.endpoints.voiceAction,
				"presence-endpoint": e.options.endpoints.voicePresence,
				"presence-events-endpoint": e.options.endpoints.voicePresenceEvents,
				"initial-tab": e.options.initialVoiceTab,
				"on-tab-change": e.options.onVoiceTabChange,
				onNotify: y
			}, null, 8, [
				"runtime-endpoint",
				"action-endpoint",
				"presence-endpoint",
				"presence-events-endpoint",
				"initial-tab",
				"on-tab-change"
			])) : o.value === "redis" ? (q(), da(fF, {
				key: 6,
				"initial-status": e.options.initialRedisStatus,
				"initial-encryption-status": e.options.initialRedisEncryptionStatus,
				"status-endpoint": e.options.endpoints.redisStatus,
				"configure-endpoint": e.options.endpoints.redisConfigure,
				"migrate-endpoint": e.options.endpoints.redisMigrateInternal,
				"encryption-status-endpoint": e.options.endpoints.redisEncryptionStatus,
				"encrypt-endpoint": e.options.endpoints.redisEncrypt,
				"decrypt-endpoint": e.options.endpoints.redisDecrypt,
				onStatus: _,
				onNotify: y
			}, null, 8, [
				"initial-status",
				"initial-encryption-status",
				"status-endpoint",
				"configure-endpoint",
				"migrate-endpoint",
				"encryption-status-endpoint",
				"encrypt-endpoint",
				"decrypt-endpoint"
			])) : o.value === "misc" ? (q(), da(TC, {
				key: 7,
				settings: e.state.misc,
				endpoint: e.options.endpoints.misc,
				onSaved: p,
				onNotify: y
			}, null, 8, ["settings", "endpoint"])) : o.value === "advanced" ? (q(), da(Dx, {
				key: 8,
				settings: e.state.advanced,
				endpoint: e.options.endpoints.advanced,
				"clear-chat-endpoint": e.options.endpoints.clearChat,
				"chat-api-url": e.options.publicEndpoints.chat,
				"models-api-url": e.options.publicEndpoints.models,
				onSaved: v,
				onNotify: y
			}, null, 8, [
				"settings",
				"endpoint",
				"clear-chat-endpoint",
				"chat-api-url",
				"models-api-url"
			])) : o.value === "system" ? (q(), da(JI, {
				key: 9,
				endpoint: e.options.endpoints.systemTasks,
				"core-run-endpoint": e.options.endpoints.coreTaskRun,
				onNotify: y
			}, null, 8, ["endpoint", "core-run-endpoint"])) : o.value === "logs" ? (q(), da(fC, {
				key: 10,
				endpoint: e.options.endpoints.logs,
				"initial-auto-scroll": e.options.initialLogAutoScroll !== !1,
				onAutoScrollChange: n[3] ||= (t) => e.options.onLogAutoScrollChange?.(t),
				onNotify: y
			}, null, 8, ["endpoint", "initial-auto-scroll"])) : Z("", !0)
		]));
	}
}), UV = ["title"], WV = { class: "tr-pill-brand" }, GV = { class: "tr-pill-copy" }, KV = {
	key: 0,
	class: "tr-pill-resources"
}, qV = { class: "tr-resource-track" }, JV = { class: "tr-pill-end" }, YV = {
	class: "tv-modal tr-modal",
	role: "dialog",
	"aria-modal": "true",
	"aria-label": "Runtime monitor"
}, XV = { class: "tr-modal-head" }, ZV = { class: "tv-eyebrow" }, QV = { class: "tr-modal-actions" }, $V = {
	class: "tr-tabs",
	"aria-label": "Runtime monitor sections"
}, eH = ["onClick"], tH = { class: "tr-tab-mark" }, nH = {
	key: 1,
	class: "tv-empty"
}, rH = {
	key: 2,
	class: "tr-tab-panel tr-overview"
}, iH = { class: "tr-overview-hero" }, aH = { class: "tr-overview-copy" }, oH = { class: "tr-overview-facts" }, sH = { class: "tr-meter-grid tr-live-meter-grid" }, cH = { class: "tr-meter-track" }, lH = { key: 0 }, uH = { class: "tr-overview-cards" }, dH = {
	key: 3,
	class: "tr-tab-panel tr-activity-view"
}, fH = { class: "tr-view-hero" }, pH = { class: "tr-view-metrics" }, mH = { class: "tr-activity-grid" }, hH = { class: "tr-runtime-section tr-activity-card hydra" }, gH = { class: "tr-inline-stats" }, _H = { class: "tr-block" }, vH = {
	key: 0,
	class: "tr-turns"
}, yH = { class: "tv-state good" }, bH = { key: 0 }, xH = { key: 1 }, SH = { key: 0 }, CH = { key: 1 }, wH = {
	key: 1,
	class: "tv-empty compact"
}, TH = { class: "tr-runtime-section tr-activity-card calls" }, EH = { class: "tr-inline-stats" }, DH = { class: "tr-block" }, OH = {
	key: 0,
	class: "tr-list"
}, kH = {
	key: 1,
	class: "tv-empty compact"
}, AH = { class: "tr-runtime-section tr-activity-card vision" }, jH = { class: "tr-inline-stats" }, MH = { class: "tr-block" }, NH = {
	key: 0,
	class: "tr-list"
}, PH = { class: "tv-state good" }, FH = {
	key: 1,
	class: "tv-empty compact"
}, IH = {
	key: 4,
	class: "tr-tab-panel tr-models-view"
}, LH = { class: "tr-view-hero" }, RH = { class: "tr-view-copy" }, zH = { class: "tr-view-metrics" }, BH = {
	key: 0,
	class: "tr-runtime-section tr-device-section"
}, VH = { class: "tr-device-grid" }, HH = { class: "tr-device-head" }, UH = { class: "tr-meter-track" }, WH = { class: "tr-runtime-section tr-model-section" }, GH = { class: "tv-state good" }, KH = {
	key: 0,
	class: "tr-model-list"
}, qH = { class: "tr-entry-mark" }, JH = { class: "tr-model-copy" }, YH = { key: 0 }, XH = {
	key: 1,
	class: "danger"
}, ZH = { class: "tr-model-action" }, QH = ["disabled", "onClick"], $H = {
	key: 1,
	class: "tv-state good"
}, eU = {
	key: 1,
	class: "tv-empty compact"
}, tU = {
	key: 5,
	class: "tr-tab-panel tr-context-view"
}, nU = { class: "tr-view-hero" }, rU = { class: "tr-view-copy" }, iU = { key: 0 }, aU = { key: 1 }, oU = { key: 2 }, sU = {
	key: 0,
	class: "tr-view-metrics"
}, cU = {
	key: 0,
	class: "tr-runtime-section tr-context-section"
}, lU = { class: "tr-context-rows" }, uU = { class: "tr-meter-track" }, dU = { class: "tr-context-stack" }, fU = {
	key: 0,
	class: "tr-context-notes"
}, pU = /* @__PURE__ */ sr({
	__name: "RuntimeStatus",
	props: {
		state: {},
		options: {},
		assistantName: {}
	},
	setup(e, { expose: t }) {
		let n = e, r = /* @__PURE__ */ U(!1), i = /* @__PURE__ */ U(!1), a = /* @__PURE__ */ U(""), o = /* @__PURE__ */ U(""), s = /* @__PURE__ */ U(""), c = /* @__PURE__ */ U(""), l = /* @__PURE__ */ U({}), u = /* @__PURE__ */ U({}), d = /* @__PURE__ */ U("connecting"), f = /* @__PURE__ */ U("overview"), p = /* @__PURE__ */ U(null), m = /* @__PURE__ */ U(""), h = 0, g = null, _ = Q(() => `${String(n.assistantName || "Tater").trim() || "Tater"} runtime`), v = Q(() => n.state.health || {}), y = Q(() => _e(v.value.loaded_models || v.value.loadedModels)), b = Q(() => _e(u.value.system)), x = Q(() => Object.keys(b.value).length ? b.value : _e(y.value.system)), S = Q(() => _e(x.value.cpu)), C = Q(() => _e(x.value.ram)), w = Q(() => _e(x.value.vram)), T = Q(() => H(y.value.loaded_count ?? I.value.loaded_count)), E = Q(() => n.state.text || `${H(v.value.verbas_enabled)} verba enabled • ${H(v.value.portals_running)} portals running • ${H(v.value.cores_running)} cores running • ${H(v.value.hydra_jobs_active ?? v.value.chat_jobs_active)} hydra jobs • ${H(v.value.llm_calls_active)} llm calls • ${H(v.value.vision_calls_active ?? v.value.voice_calls_active)} vision calls`), D = Q(() => {
			let e = H(_e(y.value.totals).estimated_total_bytes ?? _e(I.value.totals).estimated_total_bytes);
			return `${T.value} model${T.value === 1 ? "" : "s"} loaded${e > 0 ? ` • est ${Te(e)}` : ""}`;
		}), O = Q(() => n.state.health ? `${H(v.value.verbas_enabled)} Verbas · ${H(v.value.portals_running)} Portals · ${H(v.value.cores_running)} Cores` : E.value), k = Q(() => H(v.value.hydra_jobs_active ?? v.value.chat_jobs_active) + H(v.value.llm_calls_active) + H(v.value.vision_calls_active ?? v.value.voice_calls_active)), A = Q(() => ({
			connecting: "Connecting",
			live: "Live",
			reconnecting: "Reconnecting",
			paused: "Paused",
			unsupported: "Polling fallback"
		})[d.value]), j = Q(() => {
			let e = H(C.value.total_bytes), t = H(C.value.used_bytes), n = H(w.value.total_bytes), r = H(w.value.used_bytes), i = Ce(w.value.utilization_percent), a = !!(x.value.unified_memory || w.value.unified), o = [
				Ee("CPU", S.value.percent, S.value.available === !1),
				Ee("GPU", i, i === null),
				Ee(a ? "MEM" : "RAM", e > 0 ? C.value.percent ?? t / e * 100 : null, e <= 0)
			];
			return a || o.push(Ee("VRAM", n > 0 ? w.value.percent ?? r / n * 100 : null, n <= 0)), o;
		}), M = Q(() => _e(l.value.hydra_jobs || l.value.chat_jobs)), N = Q(() => _e(l.value.llm_calls)), P = Q(() => _e(l.value.vision_calls || l.value.voice_calls)), F = Q(() => _e(l.value.chat_context_window)), I = Q(() => _e(l.value.loaded_models)), ee = Q(() => Object.keys(b.value).length ? b.value : _e(I.value.system)), te = Q(() => _e(ee.value.cpu)), ne = Q(() => _e(ee.value.ram)), L = Q(() => _e(ee.value.vram)), z = Q(() => ve(I.value.models)), re = Q(() => ve(L.value.devices)), ie = Q(() => ve(M.value.active_turns)), ae = Q(() => ve(N.value.active_calls)), oe = Q(() => ve(P.value.active_calls)), se = Q(() => ie.value.length + H(N.value.running_total ?? N.value.active_total) + H(P.value.active_total)), ce = Q(() => _e(I.value.totals)), le = Q(() => Object.keys(l.value).length ? se.value : k.value), ue = Q(() => {
			if (n.state.tone === "offline") return "Backend offline";
			if (n.state.tone === "degraded") return "Runtime needs attention";
			let e = le.value;
			return e ? `${e} active operation${e === 1 ? "" : "s"}` : "All systems ready";
		}), de = Q(() => Object.keys(l.value).length ? `${H(v.value.verbas_enabled)} verba enabled • ${H(v.value.portals_running)} portals running • ${H(v.value.cores_running)} cores running • ${ie.value.length} hydra turns • ${H(N.value.running_total ?? N.value.active_total)} llm calls • ${H(P.value.active_total)} vision calls` : E.value), fe = Q(() => [
			{
				id: "overview",
				mark: "OV",
				label: "Overview",
				meta: A.value
			},
			{
				id: "activity",
				mark: "AC",
				label: "Activity",
				meta: String(se.value)
			},
			{
				id: "models",
				mark: "MO",
				label: "Models",
				meta: String(z.value.length || T.value)
			},
			{
				id: "context",
				mark: "CX",
				label: "Context",
				meta: H(F.value.prompt_tokens) ? `${Se(F.value.prompt_tokens)} tok` : "Estimate"
			}
		]), pe = Q(() => {
			let e = ce.value;
			return [
				`${H(I.value.loaded_count ?? z.value.length)} loaded`,
				H(I.value.local_llm_loaded_count) ? `${H(I.value.local_llm_loaded_count)} LLM` : "",
				H(I.value.managed_loaded_count) ? `${H(I.value.managed_loaded_count)} managed` : "",
				H(e.estimated_total_bytes) ? `est ${Te(e.estimated_total_bytes)}` : "",
				H(e.estimated_vram_bytes) ? `VRAM est ${Te(e.estimated_vram_bytes)}` : "",
				H(e.estimated_ram_bytes) ? `RAM est ${Te(e.estimated_ram_bytes)}` : "",
				H(e.estimated_unified_bytes) ? `unified est ${Te(e.estimated_unified_bytes)}` : ""
			].filter(Boolean).join(" • ") || "No loaded runtime models";
		}), me = Q(() => {
			let e = _e(F.value.breakdown), t = H(F.value.history_messages), n = H(F.value.max_history_messages) || t, r = [
				["System prompt", e.system_tokens],
				["Runtime status", e.status_tokens],
				["Core context + preamble", H(e.core_context_tokens) + H(e.platform_preamble_tokens)],
				[`Chat history (${t}/${n} msgs)`, e.history_tokens],
				["Current user turn", e.user_tokens]
			], i = H(F.value.capability_context_reserve_tokens ?? e.capability_reserve_tokens);
			return i && r.push(["Capability reserve", i]), r.map(([e, t]) => ({
				label: String(e),
				tokens: H(t)
			}));
		}), he = Q(() => [
			`Prompt ${Se(F.value.prompt_tokens)} tok`,
			`Reply budget ${Se(F.value.completion_budget_tokens)} tok`,
			H(F.value.capability_context_reserve_tokens) ? `Capability reserve ${Se(F.value.capability_context_reserve_tokens)} tok` : "",
			H(F.value.burst_context_reserve_tokens) ? `Burst reserve ${Se(F.value.burst_context_reserve_tokens)} tok` : "",
			`Min window ${Se(F.value.minimum_context_window)}`,
			`Recommended ${Se(F.value.recommended_context_window)}`
		].filter(Boolean).join(" • ")), ge = Q(() => {
			let e = _e(F.value.breakdown), t = ye(e.high_context_verba_examples).slice(0, 4);
			return [H(F.value.burst_context_reserve_tokens) ? `Recommended window includes ${Se(F.value.burst_context_reserve_tokens)} tokens of burst reserve for heavy or multi-tool turns.` : "", H(e.high_context_verbas) || H(e.heavy_cores) ? `High-context signals: ${H(e.high_context_verbas)} high-context verbas • ${H(e.heavy_cores)} heavy cores${t.length ? ` • e.g. ${t.join(", ")}` : ""}` : ""].filter(Boolean);
		});
		function _e(e) {
			return e && typeof e == "object" && !Array.isArray(e) ? e : {};
		}
		function ve(e) {
			return Array.isArray(e) ? e.filter((e) => e && typeof e == "object") : [];
		}
		function ye(e) {
			return Array.isArray(e) ? e.map((e) => xe(e)).filter(Boolean) : [];
		}
		function be(e) {
			return Array.isArray(e) ? e.map((e) => Number(e)).filter(Number.isFinite) : [];
		}
		function xe(e) {
			return String(e ?? "").trim();
		}
		function H(e) {
			let t = Number(e);
			return Number.isFinite(t) ? Math.max(0, t) : 0;
		}
		function Se(e) {
			return Math.round(H(e)).toLocaleString();
		}
		function Ce(e) {
			if (e == null || xe(e) === "") return null;
			let t = Number(e);
			return Number.isFinite(t) && t >= 0 ? Math.max(0, Math.min(100, t)) : null;
		}
		function we(e) {
			let t = Ce(e);
			return t === null ? "n/a" : `${Math.round(t)}%`;
		}
		function Te(e) {
			let t = H(e);
			if (!t) return "0 B";
			let n = [
				"B",
				"KB",
				"MB",
				"GB",
				"TB"
			], r = 0;
			for (; t >= 1024 && r < n.length - 1;) t /= 1024, r += 1;
			return `${t >= 10 || r === 0 ? t.toFixed(0) : t.toFixed(1)} ${n[r]}`;
		}
		function Ee(e, t, n) {
			let r = Ce(t);
			return {
				label: e,
				percent: r ?? 0,
				value: r === null ? "n/a" : `${Math.round(r)}%`,
				unavailable: n
			};
		}
		function De(e) {
			let t = Math.round(H(e));
			if (t < 60) return `${t}s`;
			let n = Math.floor(t / 60);
			return n < 60 ? `${n}m ${t % 60}s` : `${Math.floor(n / 60)}h ${n % 60}m`;
		}
		function Oe(e) {
			let t = H(e);
			return t ? `Loaded ${(/* @__PURE__ */ new Date(t * 1e3)).toLocaleTimeString([], {
				hour: "numeric",
				minute: "2-digit"
			})}` : "";
		}
		function ke(e) {
			let t = e.remote ? "" : H(e.estimated_bytes) ? `${xe(e.memory_kind || "ram").toUpperCase()} est ${Te(e.estimated_bytes)}` : "Estimate unavailable";
			return [
				xe(e.kind_label || e.category),
				xe(e.provider_label || e.provider || "Local"),
				xe(e.device) ? `Device ${xe(e.device)}` : "",
				t,
				Oe(e.loaded_ts)
			].filter(Boolean).join(" • ");
		}
		function Ae(e) {
			return [...ye(e.details), e.managed ? xe(e.managed_by || "Managed by settings") : ""].filter(Boolean);
		}
		function je(e) {
			return xe(e.kind_label || e.category || "Model").slice(0, 2).toUpperCase();
		}
		function Me(e) {
			let t = H(e), n = H(F.value.recommended_context_window) || H(F.value.minimum_context_window);
			return t > 0 && n > 0 ? Math.max(1, Math.min(100, t / n * 100)) : 0;
		}
		function Ne(e) {
			let t = Number(e.power_draw_w), n = Number(e.power_limit_w);
			return [
				Ce(e.utilization_percent) === null ? "GPU load n/a" : `GPU ${we(e.utilization_percent)}`,
				H(e.total_bytes) ? `${e.unified ? "GPU memory" : "VRAM"} ${Te(e.used_bytes)} / ${Te(e.total_bytes)}` : "",
				H(e.shared_memory_total_bytes) ? `Shared RAM ${Te(e.shared_memory_used_bytes)} / ${Te(e.shared_memory_total_bytes)}` : "",
				Number.isFinite(Number(e.temperature_c)) ? `${Number(e.temperature_c).toFixed(0)} C` : "",
				Number.isFinite(t) ? `${t.toFixed(0)} W${Number.isFinite(n) && n > 0 ? ` / ${n.toFixed(0)} W` : ""}` : "",
				xe(e.detail)
			].filter(Boolean).join(" • ");
		}
		function Pe(e, t) {
			return [
				`Model ${xe(e.model || "model")}`,
				xe(t === "llm" ? e.host : e.api_base),
				xe(e.activity) ? `Activity ${xe(e.activity)}` : xe(e.function) ? `Fn ${xe(e.function)}` : "",
				H(e.message_count) ? `${H(e.message_count)} msgs` : ""
			].filter(Boolean).join(" • ");
		}
		function Fe(e, t, n, r, i = "") {
			let a = H(n), o = H(t), s = Ce(r) ?? (a > 0 ? Math.max(0, Math.min(100, o / a * 100)) : null);
			return {
				label: e,
				percent: s ?? 0,
				value: r === void 0 ? a > 0 ? `${Te(o)} / ${Te(a)}` : "Unavailable" : we(r),
				unavailable: s === null,
				detail: i
			};
		}
		let Ie = Q(() => {
			let e = !!(ee.value.unified_memory || L.value.unified), t = [
				Fe("CPU Usage", 0, 0, te.value.percent, [
					H(te.value.logical_count) ? `${H(te.value.logical_count)} logical cores` : "",
					H(te.value.physical_count) ? `${H(te.value.physical_count)} physical cores` : "",
					be(te.value.load_average).length ? `load ${be(te.value.load_average).map((e) => e.toFixed(2)).join(" / ")}` : ""
				].filter(Boolean).join(" • ")),
				Fe("GPU Usage", 0, 0, L.value.utilization_percent, [
					xe(L.value.backend) ? `Backend ${xe(L.value.backend)}` : "",
					re.value.length ? `${re.value.length} device${re.value.length === 1 ? "" : "s"}` : "",
					e ? "shared/unified memory" : "",
					Ce(L.value.utilization_percent) === null ? "GPU load unavailable from this runtime" : ""
				].filter(Boolean).join(" • ")),
				Fe(e ? "Unified Memory" : "System RAM", ne.value.used_bytes, ne.value.total_bytes)
			];
			return e || t.push(Fe("System VRAM", L.value.used_bytes, L.value.total_bytes)), t;
		});
		function Le(e) {
			try {
				let t = JSON.parse(String(e.data || "{}"));
				if (!t || typeof t != "object") return;
				u.value = t, d.value = "live";
				let n = Number(t.sampled_at || 0);
				c.value = new Date(n > 0 ? n * 1e3 : Date.now()).toLocaleTimeString([], {
					hour: "numeric",
					minute: "2-digit",
					second: "2-digit"
				});
			} catch {
				d.value = "reconnecting";
			}
		}
		function Re(e = "paused") {
			g?.close(), g = null, d.value = e;
		}
		function ze() {
			if (document.visibilityState === "hidden") {
				Re("paused");
				return;
			}
			if (g) return;
			if (typeof window.EventSource != "function") {
				d.value = "unsupported";
				return;
			}
			d.value = "connecting";
			let e = new EventSource(n.options.endpoints.telemetry);
			g = e, e.addEventListener("telemetry", Le), e.onopen = () => {
				g === e && (d.value = "live");
			}, e.onerror = () => {
				g === e && (d.value = "reconnecting");
			};
		}
		function Be() {
			document.visibilityState === "hidden" ? Re("paused") : ze();
		}
		async function Ve(e = !1) {
			if (!i.value) {
				i.value = !0, a.value = "", !e && !Object.keys(l.value).length && (o.value = "Loading runtime state…");
				try {
					let t = n.options.endpoints.breakdown, r = await As(e ? t : `${t}${t.includes("?") ? "&" : "?"}refresh=true`);
					l.value = r || {}, s.value = (/* @__PURE__ */ new Date()).toLocaleTimeString([], {
						hour: "numeric",
						minute: "2-digit",
						second: "2-digit"
					}), o.value = "", n.options.onBreakdownChange?.(r || {});
				} catch (e) {
					a.value = e instanceof Error ? e.message : "Runtime breakdown failed.";
				} finally {
					i.value = !1;
				}
			}
		}
		function He() {
			Ue(), h = window.setInterval(() => {
				r.value && Ve(!0);
			}, 5e3);
		}
		function Ue() {
			h && window.clearInterval(h), h = 0;
		}
		async function We() {
			f.value = "overview", r.value = !0, await un(), p.value?.focus(), await Ve(!1), He();
		}
		function Ge() {
			r.value = !1, Ue();
		}
		async function Ke(e) {
			let t = xe(e.cache_key || e.model);
			if (!(!t || m.value)) {
				m.value = t;
				try {
					let t = H((await js(n.options.endpoints.unloadModel, {
						provider: xe(e.provider),
						model: xe(e.model),
						cache_key: xe(e.cache_key)
					})).unloaded_count), r = t ? `Unloaded ${t} local model${t === 1 ? "" : "s"}.` : "No loaded model matched.";
					n.options.onToast?.(r, "success"), await Ve(!0), n.options.onHealthRefresh?.();
				} catch (e) {
					let t = e instanceof Error ? e.message : "Model unload failed.";
					a.value = `Unload failed: ${t}`, n.options.onToast?.(a.value, "error");
				} finally {
					m.value = "";
				}
			}
		}
		function qe(e) {
			e.key === "Escape" && r.value && Ge();
		}
		return Er(() => {
			window.addEventListener("keydown", qe), document.addEventListener("visibilitychange", Be), ze();
		}), kr(() => {
			Ue(), Re("paused"), window.removeEventListener("keydown", qe), document.removeEventListener("visibilitychange", Be);
		}), t({ open: We }), (t, n) => (q(), J(K, null, [Y("button", {
			class: B(["tr-pill", [e.state.tone, `stream-${d.value}`]]),
			type: "button",
			title: `${E.value}. Open the live runtime monitor.`,
			onClick: We
		}, [
			Y("span", WV, [Y("span", GV, [Y("strong", null, V(_.value), 1), Y("small", null, V(O.value), 1)])]),
			e.state.health ? (q(), J("span", KV, [(q(!0), J(K, null, G(j.value, (e) => (q(), J("span", {
				key: e.label,
				class: B(["tr-resource", { unavailable: e.unavailable }])
			}, [Y("span", null, [Y("b", null, V(e.label), 1), Y("em", null, V(e.value), 1)]), Y("span", qV, [Y("i", { style: R({ width: `${e.percent}%` }) }, null, 4)])], 2))), 128))])) : Z("", !0),
			Y("span", JV, [Y("span", null, [Y("b", null, V(T.value), 1), n[4] ||= Y("small", null, "models", -1)]), n[5] ||= Y("i", null, "›", -1)])
		], 10, UV), ga(kl, {
			open: r.value,
			"backdrop-class": "tv-modal-backdrop tr-backdrop",
			onClose: Ge
		}, {
			default: Sn(() => [Y("section", YV, [
				Y("header", XV, [Y("div", null, [
					Y("span", ZV, V(_.value), 1),
					n[6] ||= Y("h2", null, "Live system monitor", -1),
					n[7] ||= Y("p", null, "Fast compute telemetry with focused views for current work, loaded models, and context.", -1)
				]), Y("div", QV, [
					Y("span", { class: B(["tr-live-state", `stream-${d.value}`]) }, [n[8] ||= Y("i", null, null, -1), X(V(A.value), 1)], 2),
					Y("small", null, V(c.value ? `Telemetry ${c.value}` : s.value ? `Updated ${s.value}` : "Starting live telemetry…"), 1),
					Y("button", {
						ref_key: "closeButton",
						ref: p,
						class: "tv-button",
						type: "button",
						onClick: Ge
					}, "Close", 512)
				])]),
				Y("nav", $V, [(q(!0), J(K, null, G(fe.value, (e) => (q(), J("button", {
					key: e.id,
					type: "button",
					class: B({ active: f.value === e.id }),
					onClick: (t) => f.value = e.id
				}, [
					Y("i", tH, V(e.mark), 1),
					Y("strong", null, V(e.label), 1),
					Y("small", null, V(e.meta), 1)
				], 10, eH))), 128))]),
				a.value || o.value ? (q(), J("div", {
					key: 0,
					class: B(["tv-notice", { error: !!a.value }])
				}, V(a.value || o.value), 3)) : Z("", !0),
				!Object.keys(l.value).length && i.value ? (q(), J("div", nH, "Loading runtime state…")) : f.value === "overview" ? (q(), J("section", rH, [
					Y("article", iH, [Y("div", aH, [
						n[9] ||= Y("span", { class: "tv-eyebrow" }, "Overall runtime", -1),
						Y("h3", null, V(ue.value), 1),
						Y("p", null, V(de.value), 1)
					]), Y("div", oH, [Y("div", null, [
						n[10] ||= Y("span", null, "Models", -1),
						Y("strong", null, V(T.value), 1),
						n[11] ||= Y("small", null, "loaded", -1)
					]), Y("div", null, [
						n[12] ||= Y("span", null, "Active", -1),
						Y("strong", null, V(le.value), 1),
						n[13] ||= Y("small", null, "operations", -1)
					])])]),
					Y("div", sH, [(q(!0), J(K, null, G(Ie.value, (e) => (q(), J("div", {
						key: e.label,
						class: B(["tr-meter", { unavailable: e.unavailable }])
					}, [
						Y("div", null, [Y("strong", null, V(e.label), 1), Y("span", null, V(e.value), 1)]),
						Y("span", cH, [Y("i", { style: R({ width: `${e.percent}%` }) }, null, 4)]),
						e.detail ? (q(), J("small", lH, V(e.detail), 1)) : Z("", !0)
					], 2))), 128))]),
					Y("div", uH, [
						Y("button", {
							type: "button",
							onClick: n[0] ||= (e) => f.value = "activity"
						}, [
							n[15] ||= Y("span", null, "HY", -1),
							Y("div", null, [n[14] ||= Y("small", null, "Hydra", -1), Y("strong", null, V(ie.value.length) + " active turn" + V(ie.value.length === 1 ? "" : "s"), 1)]),
							n[16] ||= Y("i", null, "›", -1)
						]),
						Y("button", {
							type: "button",
							onClick: n[1] ||= (e) => f.value = "activity"
						}, [
							n[18] ||= Y("span", null, "LL", -1),
							Y("div", null, [n[17] ||= Y("small", null, "Language models", -1), Y("strong", null, V(H(N.value.running_total ?? N.value.active_total)) + " running · " + V(H(N.value.queued_total)) + " queued", 1)]),
							n[19] ||= Y("i", null, "›", -1)
						]),
						Y("button", {
							type: "button",
							onClick: n[2] ||= (e) => f.value = "activity"
						}, [
							n[21] ||= Y("span", null, "VI", -1),
							Y("div", null, [n[20] ||= Y("small", null, "Vision", -1), Y("strong", null, V(H(P.value.active_total)) + " active call" + V(H(P.value.active_total) === 1 ? "" : "s"), 1)]),
							n[22] ||= Y("i", null, "›", -1)
						]),
						Y("button", {
							type: "button",
							onClick: n[3] ||= (e) => f.value = "models"
						}, [
							n[24] ||= Y("span", null, "MO", -1),
							Y("div", null, [n[23] ||= Y("small", null, "Local runtime", -1), Y("strong", null, V(D.value), 1)]),
							n[25] ||= Y("i", null, "›", -1)
						])
					])
				])) : f.value === "activity" ? (q(), J("section", dH, [Y("article", fH, [n[31] ||= Y("div", { class: "tr-view-copy" }, [
					Y("span", { class: "tv-eyebrow" }, "Live workload"),
					Y("h2", null, "Runtime activity"),
					Y("p", null, "See what Hydra and the model runtimes are doing now without mixing active work with lifetime totals.")
				], -1), Y("div", pH, [
					Y("div", null, [
						n[26] ||= Y("span", null, "Hydra", -1),
						Y("strong", null, V(ie.value.length), 1),
						n[27] ||= Y("small", null, "active turns", -1)
					]),
					Y("div", null, [
						n[28] ||= Y("span", null, "LLM", -1),
						Y("strong", null, V(H(N.value.running_total ?? N.value.active_total)), 1),
						Y("small", null, V(H(N.value.queued_total)) + " queued", 1)
					]),
					Y("div", null, [
						n[29] ||= Y("span", null, "Vision", -1),
						Y("strong", null, V(H(P.value.active_total)), 1),
						n[30] ||= Y("small", null, "active calls", -1)
					])
				])]), Y("div", mH, [
					Y("article", hH, [
						Y("header", null, [
							n[32] ||= Y("span", { class: "tr-section-mark" }, "HY", -1),
							n[33] ||= Y("div", null, [Y("span", { class: "tv-eyebrow" }, "Orchestration"), Y("h2", null, "Hydra Jobs")], -1),
							Y("span", { class: B(["tv-state", { good: ie.value.length > 0 }]) }, V(ie.value.length ? `${ie.value.length} active` : "Ready"), 3)
						]),
						Y("div", gH, [
							Y("span", null, [Y("b", null, V(H(M.value.total)), 1), n[34] ||= X(" total", -1)]),
							Y("span", null, [Y("b", null, V(H(M.value.webui_jobs)), 1), n[35] ||= X(" WebUI queue", -1)]),
							Y("span", null, [Y("b", null, V(H(M.value.surface_running_turns)), 1), n[36] ||= X(" surface turns", -1)])
						]),
						Y("section", _H, [n[37] ||= Y("h3", null, "Active Turns", -1), ie.value.length ? (q(), J("div", vH, [(q(!0), J(K, null, G(ie.value, (e) => (q(), J("article", { key: e.id }, [
							Y("header", null, [Y("strong", null, V(e.task_name || "Hydra task"), 1), Y("span", yH, "Running " + V(De(e.age_seconds)), 1)]),
							Y("div", null, [
								Y("span", null, V(e.platform_label || e.platform || "Unknown"), 1),
								e.source ? (q(), J("span", bH, V(e.source), 1)) : Z("", !0),
								e.id ? (q(), J("span", xH, "Drop " + V(xe(e.id).slice(0, 8)), 1)) : Z("", !0)
							]),
							e.current_tool ? (q(), J("small", SH, "Current verba/tool: " + V(e.current_tool), 1)) : Z("", !0),
							e.scope ? (q(), J("small", CH, "Scope: " + V(e.scope), 1)) : Z("", !0)
						]))), 128))])) : (q(), J("div", wH, "Hydra is ready. No active turns right now."))])
					]),
					Y("article", TH, [
						Y("header", null, [
							n[38] ||= Y("span", { class: "tr-section-mark" }, "LL", -1),
							n[39] ||= Y("div", null, [Y("span", { class: "tv-eyebrow" }, "Language models"), Y("h2", null, "LLM Calls")], -1),
							Y("span", { class: B(["tv-state", { good: H(N.value.running_total ?? N.value.active_total) > 0 }]) }, V(H(N.value.running_total ?? N.value.active_total) ? "Working" : "Ready"), 3)
						]),
						Y("div", EH, [
							Y("span", null, [Y("b", null, V(H(N.value.running_total ?? N.value.active_total)), 1), n[40] ||= X(" running", -1)]),
							Y("span", null, [Y("b", null, V(H(N.value.queued_total)), 1), n[41] ||= X(" queued", -1)]),
							Y("span", null, [Y("b", null, V(H(N.value.totals?.completed)), 1), n[42] ||= X(" completed", -1)])
						]),
						Y("section", DH, [n[43] ||= Y("h3", null, "Current Calls", -1), ae.value.length ? (q(), J("div", OH, [(q(!0), J(K, null, G(ae.value, (e, t) => (q(), J("article", { key: e.id || t }, [Y("div", null, [Y("strong", null, V(e.source_label || e.label || "Unknown source"), 1), Y("small", null, V(Pe(e, "llm")), 1)]), Y("span", { class: B(["tv-state", { good: xe(e.state) !== "queued" }]) }, V(e.state_label || (xe(e.state) === "queued" ? "Queued" : "Running")) + " " + V(De(e.state_age_seconds ?? e.age_seconds)), 3)]))), 128))])) : (q(), J("div", kH, "The LLM runtime is ready. No active calls."))])
					]),
					Y("article", AH, [
						Y("header", null, [
							n[44] ||= Y("span", { class: "tr-section-mark" }, "VI", -1),
							n[45] ||= Y("div", null, [Y("span", { class: "tv-eyebrow" }, "Visual understanding"), Y("h2", null, "Vision Calls")], -1),
							Y("span", { class: B(["tv-state", { good: H(P.value.active_total) > 0 }]) }, V(H(P.value.active_total) ? "Working" : "Ready"), 3)
						]),
						Y("div", jH, [
							Y("span", null, [Y("b", null, V(H(P.value.active_total)), 1), n[46] ||= X(" active", -1)]),
							Y("span", null, [Y("b", null, V(H(P.value.totals?.completed)), 1), n[47] ||= X(" completed", -1)]),
							Y("span", null, [Y("b", null, V(H(P.value.totals?.failed)), 1), n[48] ||= X(" failed", -1)])
						]),
						Y("section", MH, [n[49] ||= Y("h3", null, "Active Calls", -1), oe.value.length ? (q(), J("div", NH, [(q(!0), J(K, null, G(oe.value, (e, t) => (q(), J("article", { key: e.id || t }, [Y("div", null, [Y("strong", null, V(e.source_label || e.label || "Unknown source"), 1), Y("small", null, V(Pe(e, "vision")), 1)]), Y("span", PH, V(De(e.age_seconds)), 1)]))), 128))])) : (q(), J("div", FH, "Vision is ready. No active calls right now."))])
					])
				])])) : f.value === "models" ? (q(), J("section", IH, [
					Y("article", LH, [Y("div", RH, [
						n[50] ||= Y("span", { class: "tv-eyebrow" }, "Compute and memory", -1),
						n[51] ||= Y("h2", null, "Loaded Runtime Models", -1),
						Y("p", null, V(pe.value), 1)
					]), Y("div", zH, [
						Y("div", null, [
							n[52] ||= Y("span", null, "Loaded", -1),
							Y("strong", null, V(H(I.value.loaded_count ?? z.value.length)), 1),
							n[53] ||= Y("small", null, "models", -1)
						]),
						Y("div", null, [
							n[54] ||= Y("span", null, "Local LLM", -1),
							Y("strong", null, V(H(I.value.local_llm_loaded_count)), 1),
							n[55] ||= Y("small", null, "engines", -1)
						]),
						Y("div", null, [
							n[56] ||= Y("span", null, "Managed", -1),
							Y("strong", null, V(H(I.value.managed_loaded_count)), 1),
							n[57] ||= Y("small", null, "services", -1)
						]),
						Y("div", null, [
							n[58] ||= Y("span", null, "Estimated", -1),
							Y("strong", null, V(Te(ce.value.estimated_total_bytes)), 1),
							n[59] ||= Y("small", null, "memory", -1)
						])
					])]),
					re.value.length ? (q(), J("section", BH, [n[61] ||= Y("header", null, [Y("div", null, [
						Y("span", { class: "tv-eyebrow" }, "Accelerators"),
						Y("h2", null, "GPU Devices"),
						Y("p", null, "Live utilization and memory reported by the active compute backend.")
					])], -1), Y("div", VH, [(q(!0), J(K, null, G(re.value, (e, t) => (q(), J("article", { key: e.index ?? t }, [Y("div", HH, [
						n[60] ||= Y("span", { class: "tr-section-mark" }, "GPU", -1),
						Y("div", null, [Y("strong", null, V(e.name || `GPU ${e.index ?? ""}`), 1), Y("small", null, V(Ne(e)), 1)]),
						Y("b", null, V(we(e.utilization_percent)), 1)
					]), Y("span", UH, [Y("i", { style: R({ width: `${Ce(e.utilization_percent) ?? 0}%` }) }, null, 4)])]))), 128))])])) : Z("", !0),
					Y("section", WH, [Y("header", null, [n[62] ||= Y("div", null, [
						Y("span", { class: "tv-eyebrow" }, "Inventory"),
						Y("h2", null, "Loaded Model Entries"),
						Y("p", null, "Everything currently held by Tater, including managed speech and identity models.")
					], -1), Y("span", GH, V(z.value.length) + " loaded", 1)]), z.value.length ? (q(), J("div", KH, [(q(!0), J(K, null, G(z.value, (e) => (q(), J("article", {
						key: e.cache_key || `${e.provider}:${e.model}`,
						class: "tr-model-entry"
					}, [
						Y("span", qH, V(je(e)), 1),
						Y("div", JH, [
							Y("strong", null, V(e.model || "model"), 1),
							Y("small", null, V(ke(e)), 1),
							Ae(e).length ? (q(), J("small", YH, V(Ae(e).join(" • ")), 1)) : Z("", !0),
							e.warning ? (q(), J("small", XH, V(e.warning), 1)) : Z("", !0)
						]),
						Y("div", ZH, [e.unloadable && !e.managed ? (q(), J("button", {
							key: 0,
							class: "tv-button danger",
							type: "button",
							disabled: !!m.value,
							onClick: (t) => Ke(e)
						}, V(m.value === xe(e.cache_key || e.model) ? "Unloading…" : "Unload"), 9, QH)) : (q(), J("span", $H, V(e.remote ? "Spud Hub" : e.managed ? "Managed" : "Loaded"), 1))])
					]))), 128))])) : (q(), J("div", eU, "No runtime models are loaded right now."))])
				])) : (q(), J("section", tU, [Y("article", nU, [Y("div", rU, [
					n[63] ||= Y("span", { class: "tv-eyebrow" }, "Prompt budget", -1),
					n[64] ||= Y("h2", null, "Estimated Chat Context Window", -1),
					F.value.error ? (q(), J("p", iU, V(F.value.error), 1)) : H(F.value.prompt_tokens) || H(F.value.minimum_context_window) ? (q(), J("p", aU, "A clear estimate of what occupies the active Hydra prompt and how much room the selected model should provide.")) : (q(), J("p", oU, "No estimate available yet. Send a chat message so Hydra can sample the active chat prompt stack."))
				]), F.value.error ? Z("", !0) : (q(), J("div", sU, [
					Y("div", null, [
						n[65] ||= Y("span", null, "Prompt", -1),
						Y("strong", null, V(Se(F.value.prompt_tokens)), 1),
						n[66] ||= Y("small", null, "tokens", -1)
					]),
					Y("div", null, [
						n[67] ||= Y("span", null, "Reply", -1),
						Y("strong", null, V(Se(F.value.completion_budget_tokens)), 1),
						n[68] ||= Y("small", null, "budget", -1)
					]),
					Y("div", null, [
						n[69] ||= Y("span", null, "Minimum", -1),
						Y("strong", null, V(Se(F.value.minimum_context_window)), 1),
						n[70] ||= Y("small", null, "window", -1)
					]),
					Y("div", null, [
						n[71] ||= Y("span", null, "Recommended", -1),
						Y("strong", null, V(Se(F.value.recommended_context_window)), 1),
						n[72] ||= Y("small", null, "window", -1)
					])
				]))]), me.value.length && !F.value.error ? (q(), J("section", cU, [
					Y("header", null, [Y("div", null, [
						n[73] ||= Y("span", { class: "tv-eyebrow" }, "Token map", -1),
						n[74] ||= Y("h2", null, "Prompt Composition", -1),
						Y("p", null, V(he.value), 1)
					])]),
					Y("div", lU, [(q(!0), J(K, null, G(me.value, (e) => (q(), J("article", { key: e.label }, [Y("div", null, [Y("strong", null, V(e.label), 1), Y("span", null, V(Se(e.tokens)) + " tokens", 1)]), Y("span", uU, [Y("i", { style: R({ width: `${Me(e.tokens)}%` }) }, null, 4)])]))), 128))]),
					Y("div", dU, [
						Y("span", null, [Y("b", null, V(H(F.value.enabled_verbas)), 1), n[75] ||= X(" Verbas", -1)]),
						Y("span", null, [Y("b", null, V(H(F.value.connected_portals)), 1), n[76] ||= X(" Portals", -1)]),
						Y("span", null, [Y("b", null, V(H(F.value.running_cores)), 1), n[77] ||= X(" Cores", -1)])
					]),
					ge.value.length ? (q(), J("div", fU, [(q(!0), J(K, null, G(ge.value, (e, t) => (q(), J("article", { key: e }, [Y("span", null, V(t + 1), 1), Y("p", null, V(e), 1)]))), 128))])) : Z("", !0)
				])) : Z("", !0)]))
			])]),
			_: 1
		}, 8, ["open"])], 64));
	}
}), mU = {
	key: 0,
	id: "webui-auth-modal",
	class: "webui-auth-overlay active",
	"aria-hidden": "false"
}, hU = {
	class: "card webui-auth-card",
	role: "dialog",
	"aria-modal": "true",
	"aria-label": "WebUI Login"
}, gU = { class: "webui-auth-avatar-wrap" }, _U = ["src"], vU = {
	key: 1,
	class: "webui-auth-avatar-fallback"
}, yU = { class: "card-title" }, bU = { class: "small" }, xU = ["autocomplete", "disabled"], SU = { key: 0 }, CU = ["disabled"], wU = ["disabled"], TU = /* @__PURE__ */ sr({
	__name: "AuthGate",
	props: {
		state: {},
		authenticate: { type: Function }
	},
	emits: ["authenticated"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = /* @__PURE__ */ U(""), a = /* @__PURE__ */ U(""), o = /* @__PURE__ */ U(!1), s = /* @__PURE__ */ U(""), c = Q(() => !n.state.passwordSet || n.state.mode === "setup"), l = Q(() => String(n.state.username || "User").trim() || "User"), u = Q(() => l.value.match(/[A-Za-z0-9]/)?.[0]?.toUpperCase() || "U");
		async function d() {
			if (s.value = "", !i.value) {
				s.value = "Password is required.";
				return;
			}
			if (c.value && i.value.length < 4) {
				s.value = "Password must be at least 4 characters.";
				return;
			}
			if (c.value && i.value !== a.value) {
				s.value = "Passwords do not match.";
				return;
			}
			o.value = !0;
			try {
				let e = await n.authenticate({
					password: i.value,
					confirmPassword: a.value,
					setup: c.value
				});
				if (e.required) {
					s.value = e.message || "Authentication failed.";
					return;
				}
				i.value = "", a.value = "", r("authenticated", e);
			} catch (e) {
				s.value = e instanceof Error ? e.message : "Authentication failed.";
			} finally {
				o.value = !1;
			}
		}
		return On(() => n.state.required, (e) => {
			document.body.classList.toggle("webui-auth-locked", !!e), e ? document.body.classList.remove("modal-open") : s.value = "";
		}, { immediate: !0 }), kr(() => document.body.classList.remove("webui-auth-locked")), (t, n) => (q(), da(Un, { to: "body" }, [ga(lo, {
			name: "tater-popup",
			appear: ""
		}, {
			default: Sn(() => [e.state.required ? (q(), J("div", mU, [Y("div", hU, [
				Y("div", gU, [e.state.userAvatar ? (q(), J("img", {
					key: 0,
					class: "webui-auth-avatar-img",
					src: e.state.userAvatar,
					alt: "User avatar"
				}, null, 8, _U)) : (q(), J("div", vU, V(u.value), 1))]),
				Y("h3", yU, V(c.value ? "Create WebUI Password" : "WebUI Login"), 1),
				Y("div", bU, V(c.value ? `${l.value}, set a password to protect this WebUI.` : `${l.value}, enter your password to unlock TaterOS.`), 1),
				Y("form", {
					class: "form-grid webui-auth-form",
					onSubmit: bs(d, ["prevent"])
				}, [
					Y("label", null, [n[2] ||= X("Password", -1), W(Y("input", {
						"onUpdate:modelValue": n[0] ||= (e) => i.value = e,
						type: "password",
						autocomplete: c.value ? "new-password" : "current-password",
						disabled: o.value,
						autofocus: ""
					}, null, 8, xU), [[$, i.value]])]),
					c.value ? (q(), J("label", SU, [n[3] ||= X("Repeat Password", -1), W(Y("input", {
						"onUpdate:modelValue": n[1] ||= (e) => a.value = e,
						type: "password",
						autocomplete: "new-password",
						disabled: o.value
					}, null, 8, CU), [[$, a.value]])])) : Z("", !0),
					Y("button", {
						type: "submit",
						class: "action-btn",
						disabled: o.value
					}, V(o.value ? "Checking…" : c.value ? "Save Password" : "Login"), 9, wU)
				], 32),
				Y("div", { class: B(["small", { error: !!s.value }]) }, V(s.value || e.state.message || (c.value ? "Create a password and repeat it to save." : "Enter your password to continue.")), 3)
			])])) : Z("", !0)]),
			_: 1
		})]));
	}
}), EU = { class: "tater-vue-surface tvb-verbas" }, DU = { class: "tv-page-heading" }, OU = { class: "tv-heading-actions" }, kU = { class: "tv-metrics" }, AU = {
	key: 1,
	class: "tv-notice error"
}, jU = {
	class: "tv-tabs tvb-tabs",
	"aria-label": "Verba sections"
}, MU = ["onClick"], NU = { key: 0 }, PU = {
	key: 2,
	class: "tvb-card-grid"
}, FU = { class: "tv-eyebrow" }, IU = { class: "tvb-version" }, LU = {
	key: 0,
	class: "ti-tags"
}, RU = ["onClick"], zU = { key: 1 }, BU = ["onClick"], VU = {
	key: 0,
	class: "tv-empty"
}, HU = {
	key: 3,
	class: "tvb-card-grid"
}, UU = { class: "tv-eyebrow" }, WU = { class: "tv-state" }, GU = {
	key: 0,
	class: "ti-tags"
}, KU = ["onClick"], qU = {
	key: 0,
	class: "tv-empty"
}, JU = {
	key: 4,
	class: "tvb-manage-list tv-manage-workspace"
}, YU = { class: "tv-panel tvb-manage-toolbar tv-manage-hero" }, XU = { class: "tv-manage-overview" }, ZU = ["disabled"], QU = { class: "tv-manage-identity" }, $U = { class: "tv-manage-monogram" }, eW = { class: "tv-eyebrow" }, tW = { class: "tv-manage-version" }, nW = { class: "tv-manage-actions" }, rW = ["disabled", "onClick"], iW = ["onClick"], aW = {
	key: 2,
	class: "ti-purge"
}, oW = ["onUpdate:modelValue"], sW = ["onClick"], cW = {
	key: 4,
	class: "tv-state good"
}, lW = {
	key: 0,
	class: "tv-empty"
}, uW = {
	key: 5,
	class: "tv-panel tvb-repos tv-repository-manager"
}, dW = {
	class: "tv-repository-tabs",
	"aria-label": "Verba repository sources"
}, fW = {
	key: 0,
	class: "tv-trusted-repositories"
}, pW = {
	key: 0,
	class: "tv-repository-warning"
}, mW = { class: "tv-trusted-repo-grid" }, hW = {
	class: "tv-trusted-repo-card builtin selected",
	"aria-label": "Built-in Tater Shop repository"
}, gW = { class: "tv-repo-card-top" }, _W = [
	"aria-pressed",
	"onClick",
	"onKeydown"
], vW = { class: "tv-repo-check" }, yW = { class: "tv-repo-card-top" }, bW = { class: "tv-repo-monogram" }, xW = ["href"], SW = { key: 1 }, CW = {
	key: 0,
	class: "ti-tags"
}, wW = ["href"], TW = {
	key: 1,
	class: "tv-empty compact"
}, EW = {
	key: 1,
	class: "tv-custom-repositories"
}, DW = ["onClick"], OW = {
	key: 0,
	class: "tv-empty compact"
}, kW = { class: "tvb-repo-form" }, AW = { class: "tv-eyebrow" }, jW = { class: "tvb-field-grid" }, MW = /* @__PURE__ */ sr({
	__name: "VerbasApp",
	props: {
		state: {},
		options: {}
	},
	setup(e, { expose: t }) {
		let n = e, r = [
			{
				id: "installed",
				label: "Installed"
			},
			{
				id: "store",
				label: "Store"
			},
			{
				id: "manage",
				label: "Manage"
			},
			{
				id: "repos",
				label: "Repositories"
			}
		], i = /* @__PURE__ */ U(r.some((e) => e.id === n.options.initialTab) ? String(n.options.initialTab) : "installed"), a = (e) => {
			r.some((t) => t.id === e) && (i.value = e);
		}, o = /* @__PURE__ */ U(""), s = /* @__PURE__ */ U(""), c = /* @__PURE__ */ U(""), l = /* @__PURE__ */ U({}), u = /* @__PURE__ */ U(""), d = /* @__PURE__ */ U(""), f = /* @__PURE__ */ U([]), p = /* @__PURE__ */ U("trusted"), m = /* @__PURE__ */ U(null), h = /* @__PURE__ */ U({}), g = Q(() => n.state.payload?.runtime || {}), _ = Q(() => n.state.payload?.shop || {}), v = Q(() => Array.isArray(g.value.items) ? g.value.items : []), y = Q(() => Array.isArray(_.value.installed) ? _.value.installed : []), b = Q(() => Array.isArray(_.value.catalog) ? _.value.catalog : []), x = Q(() => b.value.filter((e) => !e.installed).sort(A)), S = Q(() => y.value.filter((e) => e.update_available)), C = Q(() => v.value.filter((e) => !!e.enabled).length), w = Q(() => Array.isArray(_.value.repos?.trusted) ? _.value.repos.trusted : []), T = Q(() => new Map(v.value.map((e) => [k(e.id), e]))), E = Q(() => {
			let e = /* @__PURE__ */ new Set(), t = y.value.map((t) => {
				let n = D(t.id || t.module_key || t.key);
				return e.add(k(n)), {
					id: n,
					runtime: T.value.get(k(n)) || null,
					shop: t
				};
			});
			return v.value.forEach((n) => {
				let r = D(n.id);
				r && !e.has(k(r)) && t.push({
					id: r,
					runtime: n,
					shop: null
				});
			}), t.sort((e, t) => j(e).localeCompare(j(t), void 0, {
				sensitivity: "base",
				numeric: !0
			}));
		});
		function D(e) {
			return String(e ?? "").trim();
		}
		function O(e) {
			return encodeURIComponent(D(e));
		}
		function k(e) {
			return D(e).toLowerCase();
		}
		function A(e, t) {
			return D(e.name || e.id).localeCompare(D(t.name || t.id), void 0, {
				sensitivity: "base",
				numeric: !0
			});
		}
		function j(e) {
			return D(e.runtime?.name || e.shop?.name || e.id);
		}
		function M(e) {
			return D(e.shop?.description || e.runtime?.description || "No description provided.");
		}
		function N(e) {
			return (Array.isArray(e.runtime?.platforms) && e.runtime?.platforms.length ? e.runtime.platforms : Array.isArray(e.shop?.platforms) ? e.shop.platforms : []).map((e) => D(e).replaceAll("_", " ")).filter(Boolean);
		}
		function P(e, t = "success") {
			s.value = e, c.value = t === "error" ? e : "", n.options.onToast?.(e, t);
		}
		function F() {
			let e = new Set(w.value.map((e) => D(e.url).toLowerCase()).filter(Boolean));
			f.value = Array.isArray(_.value.repos?.additional) ? _.value.repos.additional.filter((t) => !e.has(D(t.url).toLowerCase())).map((e) => ({ ...e })) : [];
		}
		function I() {
			return w.value.filter((e) => !!e.enabled).map((e) => ({
				name: D(e.name),
				url: D(e.url)
			}));
		}
		function ee() {
			return [...I(), ...f.value];
		}
		async function te(e) {
			if (o.value) return;
			let t = !!e.enabled, r = !e.enabled;
			e.enabled = r, o.value = `${r ? "Adding" : "Removing"} ${D(e.name || e.repository)}…`;
			try {
				await js(`${n.options.endpoints.shop}/repos`, { repos: ee() }), P(`${D(e.name || e.repository)} ${r ? "added to" : "removed from"} the Verba Store.`), await ne(!0);
			} catch (n) {
				e.enabled = t, P(n instanceof Error ? n.message : "Trusted repository update failed.", "error");
			} finally {
				o.value = "";
			}
		}
		async function ne(e = !1) {
			e || (o.value = "Refreshing Verba…"), c.value = "";
			try {
				let [e, t] = await Promise.all([As(n.options.endpoints.runtime), As(n.options.endpoints.shop)]);
				n.state.payload = {
					runtime: e,
					shop: t
				}, F();
			} catch (e) {
				P(e instanceof Error ? e.message : "Verba refresh failed.", "error");
			} finally {
				e || (o.value = "");
			}
		}
		async function L(e, t) {
			o.value = `${t ? "Enabling" : "Disabling"} ${e}…`;
			try {
				await js(`${n.options.endpoints.runtime}/${O(e)}/enabled`, { enabled: t }), P(`${e} ${t ? "enabled" : "disabled"}.`), await ne(!0), n.options.onHealthRefresh?.();
			} catch (e) {
				P(e instanceof Error ? e.message : "Verba toggle failed.", "error");
			} finally {
				o.value = "";
			}
		}
		async function R(e, t = "") {
			if (!(e === "remove" && !window.confirm(`Remove ${t}?${l.value[t] ? " Its saved data will also be deleted." : ""}`))) {
				o.value = `${e.replaceAll("-", " ")} ${t || "Verba"}…`, c.value = "";
				try {
					let r = t ? { id: t } : {};
					e === "remove" && (r.purge_redis = !!l.value[t]);
					let a = await js(`${n.options.endpoints.shop}/${e}`, r), o = Array.isArray(a.updated) ? a.updated.length : 0, s = Array.isArray(a.failed) ? a.failed.length : 0, c = e === "update-all" ? `Update-all completed. Updated ${o}, failed ${s}.` : "Verba action completed.";
					P(D(a.message) || c, s ? "error" : "success"), await ne(!0), e === "install" && (i.value = "installed"), n.options.onHealthRefresh?.();
				} catch (e) {
					P(e instanceof Error ? e.message : "Verba action failed.", "error");
				} finally {
					o.value = "";
				}
			}
		}
		function z(e) {
			let t = e.value ?? e.default ?? "", n = D(e.type).toLowerCase();
			if (n === "checkbox") return typeof t == "string" ? [
				"1",
				"true",
				"yes",
				"on",
				"enabled"
			].includes(t.toLowerCase()) : !!t;
			if (n === "number" || n === "range") return t === "" ? "" : Number(t);
			if (n === "multiselect") {
				if (Array.isArray(t)) return [...t];
				let e = D(t);
				if (!e) return [];
				try {
					let t = JSON.parse(e);
					if (Array.isArray(t)) return t;
				} catch {}
				return e.split(",").map((e) => e.trim()).filter(Boolean);
			}
			return t;
		}
		function re(e) {
			return (Array.isArray(e.show_when_all) ? e.show_when_all : e.show_when && typeof e.show_when == "object" ? [e.show_when] : []).every((e) => {
				let t = D(e.source_key ?? e.key);
				if (!t) return !0;
				let n = [
					...e.any_of || [],
					...e.values || [],
					...e.equals === void 0 ? [] : [e.equals],
					...e.value === void 0 ? [] : [e.value]
				].map((e) => String(e ?? "").trim());
				if (!n.length) return !0;
				let r = typeof h.value[t] == "boolean" ? h.value[t] ? "true" : "false" : String(h.value[t] ?? "").trim();
				return n.includes(r);
			});
		}
		function ie(e) {
			m.value = e, h.value = Object.fromEntries((Array.isArray(e.settings) ? e.settings : []).filter((e) => D(e.key)).map((e) => [D(e.key), z(e)]));
		}
		async function ae() {
			let e = m.value;
			if (e) {
				o.value = `Saving ${D(e.name || e.id)}…`;
				try {
					let t = Object.fromEntries((e.settings || []).filter((e) => {
						let t = D(e.type).toLowerCase();
						return D(e.key) && ![
							"section",
							"header",
							"readonly",
							"read_only",
							"led_preview"
						].includes(t) && re(e);
					}).map((e) => [D(e.key), h.value[D(e.key)]]));
					await js(`${n.options.endpoints.runtime}/${O(e.id)}/settings`, { values: t }), P(`Saved settings for ${D(e.name || e.id)}.`), m.value = null, await ne(!0);
				} catch (e) {
					P(e instanceof Error ? e.message : "Settings save failed.", "error");
				} finally {
					o.value = "";
				}
			}
		}
		function oe() {
			let e = d.value.trim();
			if (!e) {
				P("Repo URL is required.", "error");
				return;
			}
			if ([...f.value, ...w.value].some((t) => D(t.url).toLowerCase() === e.toLowerCase())) {
				P("That repository is already listed.", "error");
				return;
			}
			f.value.push({
				name: u.value.trim(),
				url: e
			}), u.value = "", d.value = "", s.value = "Repository added. Save repositories to apply it.", c.value = "";
		}
		async function se() {
			o.value = "Saving Verba repositories…";
			try {
				await js(`${n.options.endpoints.shop}/repos`, { repos: ee() }), P("Verba repositories saved."), await ne(!0);
			} catch (e) {
				P(e instanceof Error ? e.message : "Repository save failed.", "error");
			} finally {
				o.value = "";
			}
		}
		function ce(e) {
			e.key === "Escape" && (m.value = null);
		}
		return On(() => n.state.payload, F, { deep: !1 }), F(), window.addEventListener("keydown", ce), kr(() => window.removeEventListener("keydown", ce)), t({ select: a }), (e, t) => (q(), J("div", EU, [
			Y("header", DU, [t[12] ||= Y("div", null, [
				Y("span", { class: "tv-eyebrow" }, "Tater tools"),
				Y("h1", null, "Verba"),
				Y("p", null, "Enable Tater’s tools, manage their settings, and keep every Verba current.")
			], -1), Y("div", OU, [Y("span", { class: B(["tv-live-pill", { busy: !!o.value }]) }, [t[11] ||= Y("i", null, null, -1), X(V(o.value || "Ready"), 1)], 2), Y("button", {
				class: "tv-button",
				type: "button",
				onClick: t[0] ||= (e) => ne()
			}, "Refresh")])]),
			Y("div", kU, [
				Y("div", null, [t[13] ||= Y("span", null, "Installed", -1), Y("strong", null, V(y.value.length || v.value.length), 1)]),
				Y("div", null, [t[14] ||= Y("span", null, "Enabled", -1), Y("strong", null, V(C.value), 1)]),
				Y("div", null, [t[15] ||= Y("span", null, "Store", -1), Y("strong", null, V(b.value.length), 1)]),
				Y("div", null, [t[16] ||= Y("span", null, "Updates", -1), Y("strong", null, V(Number(_.value.updates_available || S.value.length)), 1)])
			]),
			s.value || c.value ? (q(), J("div", {
				key: 0,
				class: B(["tv-notice", { error: !!c.value }])
			}, V(c.value || s.value), 3)) : Z("", !0),
			_.value.errors?.length ? (q(), J("div", AU, V(_.value.errors.join(" • ")), 1)) : Z("", !0),
			Y("nav", jU, [(q(), J(K, null, G(r, (e) => Y("button", {
				key: e.id,
				type: "button",
				class: B({ active: i.value === e.id }),
				onClick: (t) => a(e.id)
			}, [X(V(e.label), 1), e.id === "manage" && S.value.length ? (q(), J("span", NU, V(S.value.length), 1)) : Z("", !0)], 10, MU)), 64))]),
			i.value === "installed" ? (q(), J("section", PU, [(q(!0), J(K, null, G(E.value, (e) => (q(), J("article", {
				key: e.id,
				class: "tv-panel tvb-verba-card"
			}, [
				Y("header", null, [Y("div", null, [Y("span", FU, V(e.id), 1), Y("h2", null, V(j(e)), 1)]), Y("span", { class: B(["tv-state", { good: e.runtime?.enabled }]) }, V(e.runtime?.enabled ? "Enabled" : "Disabled"), 3)]),
				Y("p", null, V(M(e)), 1),
				Y("div", IU, [
					Y("span", null, "Installed " + V(e.shop?.installed_ver || "0.0.0"), 1),
					Y("span", null, "Store " + V(e.shop?.store_ver || "-"), 1),
					Y("span", null, V(e.shop?.source_label || "local"), 1)
				]),
				N(e).length ? (q(), J("div", LU, [(q(!0), J(K, null, G(N(e).slice(0, 12), (e) => (q(), J("span", { key: e }, V(e), 1))), 128))])) : Z("", !0),
				Y("footer", null, [e.runtime?.settings?.length ? (q(), J("button", {
					key: 0,
					class: "tv-button",
					type: "button",
					onClick: (t) => ie(e.runtime)
				}, "Settings", 8, RU)) : (q(), J("span", zU, V(e.runtime ? "No configurable settings" : "Runtime unavailable"), 1)), e.runtime ? (q(), J("button", {
					key: 2,
					class: B(["tv-button", { primary: !e.runtime.enabled }]),
					type: "button",
					onClick: (t) => L(e.id, !e.runtime.enabled)
				}, V(e.runtime.enabled ? "Disable" : "Enable"), 11, BU)) : Z("", !0)])
			]))), 128)), E.value.length ? Z("", !0) : (q(), J("div", VU, "No installed Verba found."))])) : i.value === "store" ? (q(), J("section", HU, [(q(!0), J(K, null, G(x.value, (e) => (q(), J("article", {
				key: e.id,
				class: "tv-panel tvb-verba-card"
			}, [
				Y("header", null, [Y("div", null, [Y("span", UU, V(e.id), 1), Y("h2", null, V(e.name || e.id), 1)]), Y("span", WU, "v" + V(e.version || "-"), 1)]),
				Y("p", null, V(e.description || "No description provided."), 1),
				e.platforms?.length ? (q(), J("div", GU, [(q(!0), J(K, null, G(e.platforms.slice(0, 12), (e) => (q(), J("span", { key: e }, V(D(e).replaceAll("_", " ")), 1))), 128))])) : Z("", !0),
				Y("footer", null, [Y("span", null, V(e.source_label || "Tater Shop"), 1), Y("button", {
					class: "tv-button primary",
					type: "button",
					onClick: (t) => R("install", e.id)
				}, "Install", 8, KU)])
			]))), 128)), x.value.length ? Z("", !0) : (q(), J("div", qU, "No additional Verba are available from the configured repositories."))])) : i.value === "manage" ? (q(), J("section", JU, [
				Y("div", YU, [t[19] ||= Y("div", { class: "tv-manage-hero-copy" }, [
					Y("span", { class: "tv-eyebrow" }, "Manage library"),
					Y("h2", null, "Verba control center"),
					Y("p", null, "Keep Tater’s tools current, choose what is enabled, and remove tools you no longer use.")
				], -1), Y("div", XU, [
					Y("div", null, [t[17] ||= Y("span", null, "Installed", -1), Y("strong", null, V(y.value.length), 1)]),
					Y("div", { class: B({ attention: S.value.length }) }, [t[18] ||= Y("span", null, "Updates ready", -1), Y("strong", null, V(S.value.length), 1)], 2),
					Y("button", {
						class: "tv-button primary",
						type: "button",
						disabled: !S.value.length,
						onClick: t[1] ||= (e) => R("update-all")
					}, "Update all", 8, ZU)
				])]),
				(q(!0), J(K, null, G(y.value.slice().sort(A), (e) => (q(), J("article", {
					key: e.id,
					class: B(["tv-panel tvb-manage-row tv-manage-card", { "has-update": e.update_available }])
				}, [
					Y("div", QU, [Y("span", $U, V(D(e.name || e.id).charAt(0).toUpperCase()), 1), Y("div", null, [
						Y("span", eW, V(e.id), 1),
						Y("h3", null, V(e.name || e.id), 1),
						Y("small", null, V(e.source_label || "Local Verba"), 1)
					])]),
					Y("div", tW, [
						Y("div", null, [t[20] ||= Y("span", null, "Installed", -1), Y("strong", null, V(e.installed_ver || "0.0.0"), 1)]),
						t[22] ||= Y("i", null, "→", -1),
						Y("div", null, [t[21] ||= Y("span", null, "Latest", -1), Y("strong", null, V(e.store_ver || "-"), 1)]),
						Y("span", { class: B(["tv-state", e.update_available ? "pending" : "good"]) }, V(e.update_available ? "Update ready" : "Current"), 3)
					]),
					Y("div", nW, [
						T.value.has(k(e.id)) ? (q(), J("span", {
							key: 0,
							class: B(["tv-manage-runtime", { online: T.value.get(k(e.id))?.enabled }])
						}, [t[23] ||= Y("i", null, null, -1), X(V(T.value.get(k(e.id))?.enabled ? "Enabled" : "Disabled"), 1)], 2)) : Z("", !0),
						Y("button", {
							class: B(["tv-button", { primary: e.update_available }]),
							type: "button",
							disabled: !e.update_available,
							onClick: (t) => R("update", e.id)
						}, V(e.update_available ? "Update" : "Current"), 11, rW),
						T.value.has(k(e.id)) ? (q(), J("button", {
							key: 1,
							class: "tv-button",
							type: "button",
							onClick: (t) => L(e.id, !T.value.get(k(e.id))?.enabled)
						}, V(T.value.get(k(e.id))?.enabled ? "Disable" : "Enable"), 9, iW)) : Z("", !0),
						e.required ? Z("", !0) : (q(), J("label", aW, [W(Y("input", {
							"onUpdate:modelValue": (t) => l.value[e.id] = t,
							type: "checkbox"
						}, null, 8, oW), [[cs, l.value[e.id]]]), t[24] ||= X(" Delete data", -1)])),
						e.required ? (q(), J("span", cW, "Required")) : (q(), J("button", {
							key: 3,
							class: "tv-button danger",
							type: "button",
							onClick: (t) => R("remove", e.id)
						}, "Remove", 8, sW))
					])
				], 2))), 128)),
				y.value.length ? Z("", !0) : (q(), J("div", lW, "No installed Verba found."))
			])) : (q(), J("section", uW, [
				t[37] ||= Y("header", { class: "tv-repository-heading" }, [Y("div", null, [
					Y("span", { class: "tv-eyebrow" }, "Repository library"),
					Y("h2", null, "Verba repositories"),
					Y("p", null, "Choose a Tater-trusted source or add your own manifest.")
				])], -1),
				Y("nav", dW, [Y("button", {
					type: "button",
					class: B({ active: p.value === "trusted" }),
					onClick: t[2] ||= (e) => p.value = "trusted"
				}, "Trusted repositories", 2), Y("button", {
					type: "button",
					class: B({ active: p.value === "custom" }),
					onClick: t[3] ||= (e) => p.value = "custom"
				}, "Custom repositories", 2)]),
				p.value === "trusted" ? (q(), J("div", fW, [
					t[33] ||= Y("p", { class: "tv-repository-intro" }, "Curated sources are reviewed by Tater. Select a card to add or remove its Verba from your Store automatically.", -1),
					_.value.repos?.trusted_error ? (q(), J("div", pW, "The trusted directory is temporarily unavailable. Your enabled repositories are unchanged.")) : Z("", !0),
					Y("div", mW, [Y("article", hW, [
						t[28] ||= Y("span", { class: "tv-repo-check" }, "✓", -1),
						Y("div", gW, [t[27] ||= Y("span", { class: "tv-repo-monogram" }, "T", -1), Y("div", null, [
							t[25] ||= Y("span", { class: "tv-eyebrow" }, "Always available", -1),
							Y("h3", null, V(_.value.repos?.default?.name || "Tater Shop"), 1),
							t[26] ||= Y("p", null, [X("by "), Y("strong", null, "Tater Assistant")], -1)
						])]),
						t[29] ||= Y("p", null, "The official built-in Verba catalog.", -1),
						t[30] ||= Y("footer", null, [Y("span", { class: "tv-repo-enabled" }, "Built in")], -1)
					]), (q(!0), J(K, null, G(w.value, (e) => (q(), J("article", {
						key: e.id || e.url,
						class: B(["tv-trusted-repo-card", { selected: e.enabled }]),
						role: "button",
						tabindex: "0",
						"aria-pressed": !!e.enabled,
						onClick: (t) => te(e),
						onKeydown: [Ss(bs((t) => te(e), ["prevent"]), ["enter"]), Ss(bs((t) => te(e), ["prevent"]), ["space"])]
					}, [
						Y("span", vW, V(e.enabled ? "✓" : "+"), 1),
						Y("div", yW, [Y("span", bW, V(D(e.repository || e.name).charAt(0).toUpperCase()), 1), Y("div", null, [
							t[32] ||= Y("span", { class: "tv-eyebrow" }, "Trusted Verba source", -1),
							Y("h3", null, V(e.repository || e.name), 1),
							Y("p", null, [t[31] ||= X("by ", -1), e.author_url ? (q(), J("a", {
								key: 0,
								href: e.author_url,
								target: "_blank",
								rel: "noreferrer",
								onClick: t[4] ||= bs(() => {}, ["stop"])
							}, V(e.author || "Community author"), 9, xW)) : (q(), J("strong", SW, V(e.author || "Community author"), 1))])
						])]),
						Y("p", null, V(e.description || "Additional Verba for the Tater Store."), 1),
						e.tags?.length ? (q(), J("div", CW, [(q(!0), J(K, null, G(e.tags, (e) => (q(), J("span", { key: e }, V(e), 1))), 128))])) : Z("", !0),
						Y("footer", null, [e.homepage ? (q(), J("a", {
							key: 0,
							href: e.homepage,
							target: "_blank",
							rel: "noreferrer",
							onClick: t[5] ||= bs(() => {}, ["stop"])
						}, "View repository ↗", 8, wW)) : Z("", !0), Y("span", { class: B(e.enabled ? "tv-repo-enabled" : "tv-repo-available") }, V(e.enabled ? "Added to Store" : "Select to add"), 3)])
					], 42, _W))), 128))]),
					w.value.length ? Z("", !0) : (q(), J("div", TW, "No additional trusted Verba repositories are listed yet."))
				])) : (q(), J("div", EW, [
					t[36] ||= Y("p", { class: "tv-repository-intro" }, "Custom manifests are managed by you and are not reviewed by Tater.", -1),
					(q(!0), J(K, null, G(f.value, (e, t) => (q(), J("article", {
						key: `${e.url}-${t}`,
						class: "ti-repo-row"
					}, [Y("div", null, [Y("strong", null, V(e.name || "Custom repository"), 1), Y("code", null, V(e.url), 1)]), Y("button", {
						class: "tv-button",
						type: "button",
						onClick: (e) => f.value.splice(t, 1)
					}, "Remove", 8, DW)]))), 128)),
					f.value.length ? Z("", !0) : (q(), J("div", OW, "No custom repositories configured.")),
					Y("div", kW, [
						Y("label", null, [t[34] ||= Y("span", null, "Name (optional)", -1), W(Y("input", {
							"onUpdate:modelValue": t[6] ||= (e) => u.value = e,
							type: "text",
							placeholder: "My Verba Repo"
						}, null, 512), [[$, u.value]])]),
						Y("label", null, [t[35] ||= Y("span", null, "Manifest URL", -1), W(Y("input", {
							"onUpdate:modelValue": t[7] ||= (e) => d.value = e,
							type: "url",
							placeholder: "https://example.com/verbas.json",
							onKeyup: Ss(oe, ["enter"])
						}, null, 544), [[$, d.value]])]),
						Y("button", {
							class: "tv-button",
							type: "button",
							onClick: oe
						}, "Add"),
						Y("button", {
							class: "tv-button primary",
							type: "button",
							onClick: se
						}, "Save custom repositories")
					])
				]))
			])),
			ga(kl, {
				open: !!m.value,
				onClose: t[10] ||= (e) => m.value = null
			}, {
				default: Sn(() => [Y("form", {
					class: "tv-modal tvb-settings-modal",
					onSubmit: bs(ae, ["prevent"])
				}, [
					Y("header", null, [Y("div", null, [Y("span", AW, V(m.value?.id), 1), Y("h2", null, V(m.value?.name || m.value?.id) + " settings", 1)]), Y("button", {
						class: "tv-button",
						type: "button",
						onClick: t[8] ||= (e) => m.value = null
					}, "Close")]),
					Y("div", jW, [(q(!0), J(K, null, G(m.value?.settings || [], (e, n) => (q(), da(zd, {
						key: e.key || n,
						modelValue: h.value[e.key],
						"onUpdate:modelValue": (t) => h.value[e.key] = t,
						field: e,
						"all-values": h.value,
						onError: t[9] ||= (e) => P(e, "error"),
						onNotify: P
					}, null, 8, [
						"modelValue",
						"onUpdate:modelValue",
						"field",
						"all-values"
					]))), 128))]),
					Y("footer", null, [Y("span", null, V(o.value || s.value), 1), t[38] ||= Y("button", {
						class: "tv-button primary",
						type: "submit"
					}, "Save settings", -1)])
				], 32)]),
				_: 1
			}, 8, ["open"])
		]));
	}
}), NW = {
	id: "app-sidebar",
	class: "sidebar"
}, PW = { class: "sidebar-controls" }, FW = ["disabled", "aria-label"], IW = { class: "brand-wrap" }, LW = { id: "brand-name" }, RW = { id: "brand-subtitle" }, zW = {
	class: "nav-stack",
	"aria-label": "Primary"
}, BW = ["data-view", "onClick"], VW = ["aria-label"], HW = { class: "main-pane" }, UW = { class: "topbar" }, WW = { id: "view-title" }, GW = { id: "view-subtitle" }, KW = {
	id: "runtime-summary",
	class: "runtime-summary runtime-summary-vue-host"
}, qW = ["data-view"], JW = {
	key: 0,
	class: "tv-notice error"
}, YW = {
	key: 1,
	class: "tv-empty"
}, XW = {
	key: 2,
	class: "tater-shell-refresh-indicator",
	role: "status"
}, ZW = ["disabled"], QW = {
	id: "toast-root",
	class: "toast-root",
	"aria-live": "polite",
	"aria-atomic": "true"
}, $W = ["onClick"], eG = /* @__PURE__ */ sr({
	__name: "AppShell",
	props: { options: {} },
	setup(e, { expose: t }) {
		let n = e, r = [
			{
				id: "dashboard",
				label: "Dashboard",
				subtitle: "Tater status, live signals, and generated briefs."
			},
			{
				id: "chat",
				label: "Chat",
				subtitle: "Talk to Tater Totterson"
			},
			{
				id: "verbas",
				label: "Verba",
				subtitle: "Enable tools and manage Verba settings + shop updates."
			},
			{
				id: "portals",
				label: "Portals",
				subtitle: "Portal runtime controls and full Portal Shop manager."
			},
			{
				id: "cores",
				label: "Cores",
				subtitle: "Core runtime controls and full Core Shop manager."
			},
			{
				id: "integrations",
				label: "Integrations",
				subtitle: "Service endpoints, credentials, devices, runtime, and integration updates."
			},
			{
				id: "spudex",
				label: "Spudex",
				subtitle: "Policy-controlled terminal sessions for Tater."
			},
			{
				id: "settings",
				label: "Settings",
				subtitle: "Global WebUI and Tater runtime configuration."
			}
		], i = new Set(r.map((e) => e.id)), a = Ft({
			dashboard: Kg,
			chat: Dc,
			verbas: MW,
			portals: Dy,
			cores: _g,
			integrations: bv,
			spudex: lx,
			settings: HV
		}), o = n.options.initialBranding || {}, s = /* @__PURE__ */ U(j(n.options.initialView)), c = /* @__PURE__ */ Dt({}), l = /* @__PURE__ */ U(""), u = /* @__PURE__ */ U(""), d = /* @__PURE__ */ U(null), f = /* @__PURE__ */ U(null), p = /* @__PURE__ */ Et({
			firstName: String(o.firstName || "Tater"),
			fullName: String(o.fullName || "Tater Totterson"),
			version: String(o.version || ""),
			versionLabel: String(o.versionLabel || "")
		}), m = /* @__PURE__ */ Et({
			health: n.options.initialRuntimeState?.health || null,
			text: n.options.initialRuntimeState?.text || "Checking system…",
			tone: n.options.initialRuntimeState?.tone || "normal"
		}), h = /* @__PURE__ */ Et({ ...n.options.initialAuthState }), g = /* @__PURE__ */ U(!!n.options.initialSidebarCollapsed), _ = /* @__PURE__ */ U(""), v = 0, y = 0, b = 0, x = /* @__PURE__ */ new Map(), S = /* @__PURE__ */ U([]), C = Q(() => r.find((e) => e.id === s.value) || r[0]), w = Q(() => c[s.value] || null), T = Q(() => a[s.value]), E = Q(() => w.value?.state || {}), D = Q(() => w.value?.options || {}), O = Q(() => ({
			"sidebar-collapsed": g.value,
			"sidebar-collapsing": _.value === "collapsing",
			"sidebar-expanding": _.value === "expanding"
		})), k = Q(() => {
			let e = p.firstName || "Tater";
			if (p.versionLabel) return `${e} ${p.versionLabel}`;
			let t = String(p.version || "").replace(/^v/i, "");
			return t ? `${e} v${t}` : "";
		}), A = Q(() => s.value === "chat" ? `Talk to ${p.fullName || p.firstName || "Tater"}` : s.value === "settings" ? `Global WebUI and ${p.firstName || "Tater"} runtime configuration.` : C.value.subtitle);
		function j(e) {
			let t = String(e || "").trim().toLowerCase();
			return i.has(t) ? t : "dashboard";
		}
		function M() {
			let e = String(window.location.hash || "").match(/^#\/?([a-z-]+)/i);
			if (e && i.has(e[1].toLowerCase())) return e[1].toLowerCase();
			let t = new URLSearchParams(window.location.search).get("view");
			return t && i.has(t.toLowerCase()) ? t.toLowerCase() : null;
		}
		function N(e, t) {
			if (t === "none") return;
			let n = new URL(window.location.href);
			n.hash = `/${e}`;
			let r = t === "replace" ? "replaceState" : "pushState";
			window.history[r]({
				...window.history.state || {},
				taterView: e
			}, "", n);
		}
		function P(e, t) {
			let n = c[e];
			if (n) {
				Object.keys(n.state).forEach((e) => {
					e in t.state || delete n.state[e];
				}), Object.assign(n.state, t.state || {}), Object.assign(n.options, t.options || {});
				return;
			}
			c[e] = {
				state: /* @__PURE__ */ Et(t.state || {}),
				options: Ft(t.options || {})
			};
		}
		async function F(e, t = !1) {
			if (c[e] && !t) return;
			let r = ++y;
			l.value = e, u.value = "";
			try {
				let i = await n.options.loadView(e, { refresh: t });
				if (r !== y) return;
				P(e, i);
			} catch (t) {
				if (r !== y) return;
				u.value = t instanceof Error ? t.message : `Could not load ${e}.`;
			} finally {
				r === y && (l.value = "");
			}
		}
		async function I(e, t = {}) {
			let r = j(e), i = s.value !== r;
			!i && c[r] && !t.refresh || (s.value = r, document.body.dataset.view = r, n.options.onViewChange?.(r), N(r, i ? t.history || "push" : t.history === "none" ? "none" : "replace"), !h.required && (await F(r, !!t.refresh), await un()));
		}
		async function ee() {
			let e = d.value;
			typeof e?.refresh == "function" ? await e.refresh() : await F(s.value, !0);
		}
		async function te(e) {
			let t = d.value;
			typeof t?.refreshTab == "function" ? await t.refreshTab(e) : await ee();
		}
		async function ne(e, t, n = "") {
			await I(e), await un(), await d.value?.select?.(t, n), await un();
		}
		function L(e) {
			d.value?.select?.(e);
		}
		function R(e, t) {
			let n = j(e), r = c[n];
			r && ([
				"dashboard",
				"verbas",
				"portals",
				"cores",
				"spudex"
			].includes(n) ? r.state.payload = t : n === "integrations" ? r.state.settings = t : Object.assign(r.state, t || {}));
		}
		function z(e) {
			R(s.value, e);
		}
		function re(e) {
			e.firstName !== void 0 && (p.firstName = String(e.firstName || "Tater")), e.fullName !== void 0 && (p.fullName = String(e.fullName || p.firstName || "Tater")), e.version !== void 0 && (p.version = String(e.version || "")), e.versionLabel !== void 0 && (p.versionLabel = String(e.versionLabel || ""));
		}
		function ie(e) {
			Object.assign(h, e || {});
		}
		function ae(e, t = "Session expired. Please log in again.") {
			y += 1, l.value = "", Object.assign(h, e || {}, {
				required: !0,
				authenticated: !1,
				message: t
			});
		}
		async function oe(e) {
			Object.assign(h, e, {
				required: !1,
				authenticated: !0,
				message: ""
			}), await F(s.value, !c[s.value]), await n.options.onAuthenticated?.(h);
		}
		function se(e, t = "normal") {
			m.health = e || {}, m.text = "", m.tone = t;
		}
		function ce(e, t = "normal") {
			m.health = null, m.text = String(e || "").trim(), m.tone = t;
		}
		async function le() {
			await f.value?.open?.();
		}
		function ue(e) {
			let t = S.value.find((t) => t.id === e);
			if (!t || t.closing) return;
			t.visible = !1, t.closing = !0;
			let n = x.get(e);
			n && window.clearTimeout(n), x.set(e, window.setTimeout(() => {
				S.value = S.value.filter((t) => t.id !== e), x.delete(e);
			}, 420));
		}
		function de(e, t = "success", n = 2600) {
			let r = String(e || "").trim();
			if (!r) return;
			let i = ++b;
			S.value.push({
				id: i,
				message: r,
				tone: t === "error" ? "error" : "success",
				visible: !1,
				closing: !1
			}), requestAnimationFrame(() => {
				let e = S.value.find((e) => e.id === i);
				e && (e.visible = !0);
			}), x.set(i, window.setTimeout(() => ue(i), Math.max(1200, Number(n) || 2600)));
		}
		function fe(e) {
			let t = String(document.body.dataset.popupEffect || "flame"), n = {
				disabled: [120, 140],
				flame: [460, 480],
				dust: [500, 520],
				glitch: [340, 360],
				portal: [500, 520],
				melt: [480, 500]
			};
			return (n[t] || n.flame)[e === "collapse" ? 0 : 1];
		}
		function pe(e) {
			let t = !!e;
			if (v && window.clearTimeout(v), !(window.matchMedia?.("(min-width: 981px)").matches ?? window.innerWidth > 980)) {
				g.value = t, _.value = "", n.options.onSidebarChange?.(t);
				return;
			}
			_.value = t ? "collapsing" : "expanding", t || (g.value = !1), v = window.setTimeout(() => {
				g.value = t, _.value = "", v = 0, n.options.onSidebarChange?.(t);
			}, fe(t ? "collapse" : "expand"));
		}
		function me() {
			let e = M();
			e && I(e, { history: "none" });
		}
		return t({
			navigate: I,
			refresh: ee,
			refreshTab: te,
			selectViewTab: ne,
			selectSettings: L,
			select: L,
			updateView: R,
			update: z,
			setHealth: se,
			setStatus: ce,
			openRuntime: le,
			toast: de,
			updateBranding: re,
			setSidebarCollapsed: pe,
			updateAuth: ie,
			requireAuth: ae
		}), Er(() => {
			window.addEventListener("popstate", me), I(M() || s.value, { history: "replace" });
		}), kr(() => {
			window.removeEventListener("popstate", me), v && window.clearTimeout(v), x.forEach((e) => window.clearTimeout(e)), x.clear();
		}), (t, n) => (q(), J(K, null, [
			n[3] ||= Y("div", {
				class: "bg-shape bg-shape-a",
				"aria-hidden": "true"
			}, null, -1),
			n[4] ||= Y("div", {
				class: "bg-shape bg-shape-b",
				"aria-hidden": "true"
			}, null, -1),
			Y("div", {
				id: "app-shell",
				class: B(["app-shell tater-vue-shell", O.value])
			}, [Y("aside", NW, [
				Y("div", PW, [Y("button", {
					id: "sidebar-collapse-btn",
					class: "inline-btn sidebar-toggle-btn",
					type: "button",
					disabled: !!_.value,
					"aria-label": g.value ? "Show menu" : "Hide menu",
					onClick: n[0] ||= (e) => pe(!g.value)
				}, V(_.value ? "⋯" : g.value ? "☰" : "✕"), 9, FW)]),
				Y("div", IW, [Y("div", null, [Y("h1", LW, V(p.firstName), 1), Y("p", RW, V(p.firstName) + "OS Control Surface", 1)])]),
				Y("nav", zW, [(q(), J(K, null, G(r, (e) => Y("button", {
					key: e.id,
					class: B(["nav-btn", { active: s.value === e.id }]),
					"data-view": e.id,
					type: "button",
					onClick: (t) => I(e.id)
				}, V(e.label), 11, BW)), 64))]),
				k.value ? (q(), J("div", {
					key: 0,
					id: "tater-build-version",
					class: "sidebar-build-version",
					"aria-label": `Current ${p.firstName || "Tater"} version`,
					"aria-live": "polite"
				}, V(k.value), 9, VW)) : Z("", !0)
			]), Y("main", HW, [Y("header", UW, [Y("div", null, [Y("h2", WW, V(C.value.label), 1), Y("p", GW, V(A.value), 1)]), Y("div", KW, [ga(pU, {
				ref_key: "runtimeComponent",
				ref: f,
				state: m,
				options: e.options.runtimeOptions,
				"assistant-name": p.firstName
			}, null, 8, [
				"state",
				"options",
				"assistant-name"
			])])]), Y("section", {
				id: "view-root",
				class: "view-root tater-shell-view-root",
				"data-view": s.value
			}, [
				u.value && !w.value ? (q(), J("div", JW, [X("Failed to load " + V(C.value.label) + ": " + V(u.value) + " ", 1), Y("button", {
					class: "inline-btn",
					type: "button",
					onClick: n[1] ||= (e) => F(s.value, !0)
				}, "Try again")])) : w.value ? Z("", !0) : (q(), J("div", YW, "Loading " + V(C.value.label) + "…", 1)),
				(q(), da(hr, { max: r.length }, [w.value ? (q(), da(Rr(T.value), {
					ref_key: "activeComponent",
					ref: d,
					key: s.value,
					state: E.value,
					options: D.value
				}, null, 8, ["state", "options"])) : Z("", !0)], 1032, ["max"])),
				l.value && w.value ? (q(), J("div", XW, "Refreshing " + V(C.value.label) + "…", 1)) : Z("", !0)
			], 8, qW)])], 2),
			Y("button", {
				id: "sidebar-expand-btn",
				class: "sidebar-expand-fab",
				type: "button",
				"aria-label": "Show menu",
				title: "Show menu",
				disabled: !!_.value,
				onClick: n[2] ||= (e) => pe(!1)
			}, V(_.value ? "⋯" : "☰"), 9, ZW),
			Y("div", QW, [(q(!0), J(K, null, G(S.value, (e) => (q(), J("button", {
				key: e.id,
				type: "button",
				class: B(["toast-item", [e.tone, {
					show: e.visible,
					"flame-out": e.closing
				}]]),
				onClick: (t) => ue(e.id)
			}, V(e.message), 11, $W))), 128))]),
			ga(TU, {
				state: h,
				authenticate: e.options.authenticate,
				onAuthenticated: oe
			}, null, 8, ["state", "authenticate"])
		], 64));
	}
});
//#endregion
//#region src/entry.ts
function tG(e, t) {
	let n = Es(eG, { options: t }), r = n.mount(e);
	return {
		navigate: (e, t) => r.navigate(e, t),
		refresh: () => r.refresh(),
		refreshTab: (e) => r.refreshTab(e),
		selectViewTab: (e, t, n) => r.selectViewTab(e, t, n),
		selectSettings: (e) => r.selectSettings(e),
		select: (e) => r.select(e),
		updateView: (e, t) => r.updateView(e, t),
		update: (e) => r.update(e),
		setHealth: (e, t) => r.setHealth(e, t),
		setStatus: (e, t) => r.setStatus(e, t),
		openRuntime: () => r.openRuntime(),
		toast: (e, t, n) => r.toast(e, t, n),
		updateBranding: (e) => r.updateBranding(e),
		setSidebarCollapsed: (e) => r.setSidebarCollapsed(e),
		updateAuth: (e) => r.updateAuth(e),
		requireAuth: (e, t) => r.requireAuth(e, t),
		unmount() {
			n.unmount();
		}
	};
}
function nG(e, t) {
	let n = /* @__PURE__ */ Et({
		profile: t.initialProfile || {},
		messages: t.initialMessages || [],
		stats: t.initialStats || {
			enabled: !1,
			stats: null
		}
	}), r = Es(Dc, {
		state: n,
		options: t
	});
	return r.mount(e), {
		update(e) {
			e.profile && (n.profile = e.profile), e.messages && (n.messages = e.messages), e.stats && (n.stats = e.stats);
		},
		unmount() {
			r.unmount();
		}
	};
}
function rG(e, t) {
	let n = /* @__PURE__ */ Et({ payload: t.initialPayload }), r = Es(Kg, {
		state: n,
		options: t
	});
	return r.mount(e), {
		update(e) {
			n.payload = e;
		},
		unmount() {
			r.unmount();
		}
	};
}
function iG(e, t) {
	let n = /* @__PURE__ */ Et({ settings: t.initialSettings }), r = Es(bv, {
		state: n,
		options: t
	});
	return r.mount(e), {
		update(e) {
			n.settings = e;
		},
		unmount() {
			r.unmount();
		}
	};
}
function aG(e, t) {
	let n = /* @__PURE__ */ Et({ payload: t.initialPayload }), r = Es(MW, {
		state: n,
		options: t
	});
	return r.mount(e), {
		update(e) {
			n.payload = e;
		},
		unmount() {
			r.unmount();
		}
	};
}
function oG(e, t) {
	let n = /* @__PURE__ */ Et({ payload: t.initialPayload }), r = Es(Dy, {
		state: n,
		options: t
	});
	return r.mount(e), {
		update(e) {
			n.payload = e;
		},
		unmount() {
			r.unmount();
		}
	};
}
function sG(e, t) {
	let n = /* @__PURE__ */ Et({ payload: t.initialPayload }), r = Es(_g, {
		state: n,
		options: t
	}), i = r.mount(e);
	return {
		update(e) {
			n.payload = e;
		},
		refresh() {
			return i.refresh?.() || Promise.resolve();
		},
		refreshTab(e) {
			return i.refreshTab?.(e) || Promise.resolve();
		},
		unmount() {
			r.unmount();
		}
	};
}
function cG(e, t) {
	let n = /* @__PURE__ */ Et({ payload: t.initialPayload }), r = Es(lx, {
		state: n,
		options: t
	}), i = r.mount(e);
	return {
		update(e) {
			n.payload = e;
		},
		refresh() {
			return i.refresh?.() || Promise.resolve();
		},
		unmount() {
			r.unmount();
		}
	};
}
function lG(e, t) {
	let n = /* @__PURE__ */ Et({
		summary: t.initialSummary || {},
		general: t.initialGeneral || {},
		hydra: t.initialHydra || {},
		misc: t.initialMisc || {},
		people: t.initialPeople || {},
		spudLink: t.initialSpudLink || {},
		models: t.initialModels || {},
		advanced: t.initialAdvanced || {}
	}), r = Es(HV, {
		state: n,
		options: t
	}), i = r.mount(e);
	return {
		update(e) {
			n.summary = e;
		},
		select(e) {
			i.select?.(e);
		},
		unmount() {
			r.unmount();
		}
	};
}
function uG(e, t) {
	let n = /* @__PURE__ */ Et({
		health: t.initialState?.health || null,
		text: t.initialState?.text || "Checking system…",
		tone: t.initialState?.tone || "normal"
	}), r = Es(pU, {
		state: n,
		options: t
	}), i = r.mount(e);
	return {
		setHealth(e, t = "normal") {
			n.health = e || {}, n.text = "", n.tone = t;
		},
		setStatus(e, t = "normal") {
			n.health = null, n.text = String(e || "").trim(), n.tone = t;
		},
		open() {
			return i.open?.() || Promise.resolve();
		},
		unmount() {
			r.unmount();
		}
	};
}
function dG(e, t) {
	let n = /* @__PURE__ */ Et({ payload: t.initialPayload }), r = Es(dd, {
		state: n,
		options: t
	});
	return r.mount(e), {
		update(e) {
			n.payload = e;
		},
		unmount() {
			r.unmount();
		}
	};
}
//#endregion
export { tG as mountAppShell, nG as mountChat, sG as mountCores, rG as mountDashboard, iG as mountIntegrations, dG as mountMusicCore, oG as mountPortals, uG as mountRuntimeStatus, lG as mountSettings, cG as mountSpudex, aG as mountVerbas };
