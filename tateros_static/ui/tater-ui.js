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
}, l = Object.prototype.hasOwnProperty, u = (e, t) => l.call(e, t), d = Array.isArray, f = (e) => x(e) === "[object Map]", p = (e) => x(e) === "[object Set]", m = (e) => x(e) === "[object Date]", h = (e) => typeof e == "function", g = (e) => typeof e == "string", _ = (e) => typeof e == "symbol", v = (e) => typeof e == "object" && !!e, y = (e) => (v(e) || h(e)) && h(e.then) && h(e.catch), b = Object.prototype.toString, x = (e) => b.call(e), S = (e) => x(e).slice(8, -1), C = (e) => x(e) === "[object Object]", w = (e) => g(e) && e !== "NaN" && e[0] !== "-" && "" + parseInt(e, 10) === e, T = /* @__PURE__ */ e(",key,ref,ref_for,ref_key,onVnodeBeforeMount,onVnodeMounted,onVnodeBeforeUpdate,onVnodeUpdated,onVnodeBeforeUnmount,onVnodeUnmounted"), E = (e) => {
	let t = /* @__PURE__ */ Object.create(null);
	return ((n) => t[n] || (t[n] = e(n)));
}, D = /-\w/g, O = E((e) => e.replace(D, (e) => e.slice(1).toUpperCase())), k = /\B([A-Z])/g, A = E((e) => e.replace(k, "-$1").toLowerCase()), ee = E((e) => e.charAt(0).toUpperCase() + e.slice(1)), j = E((e) => e ? `on${ee(e)}` : ""), M = (e, t) => !Object.is(e, t), te = (e, ...t) => {
	for (let n = 0; n < e.length; n++) e[n](...t);
}, N = (e, t, n, r = !1) => {
	Object.defineProperty(e, t, {
		configurable: !0,
		enumerable: !1,
		writable: r,
		value: n
	});
}, ne = (e) => {
	let t = parseFloat(e);
	return isNaN(t) ? e : t;
}, re = (e) => {
	let t = g(e) ? Number(e) : NaN;
	return isNaN(t) ? e : t;
}, P, F = () => P ||= typeof globalThis < "u" ? globalThis : typeof self < "u" ? self : typeof window < "u" ? window : typeof global < "u" ? global : {};
function I(e) {
	if (d(e)) {
		let t = {};
		for (let n = 0; n < e.length; n++) {
			let r = e[n], i = g(r) ? R(r) : I(r);
			if (i) for (let e in i) t[e] = i[e];
		}
		return t;
	}
	if (g(e) || v(e)) return e;
}
var L = /;(?![^(]*\))/g, ie = /:([^]+)/, ae = /\/\*[^]*?\*\//g;
function R(e) {
	let t = {};
	return e.replace(ae, "").split(L).forEach((e) => {
		if (e) {
			let n = e.split(ie);
			n.length > 1 && (t[n[0].trim()] = n[1].trim());
		}
	}), t;
}
function z(e) {
	let t = "";
	if (g(e)) t = e;
	else if (d(e)) for (let n = 0; n < e.length; n++) {
		let r = z(e[n]);
		r && (t += r + " ");
	}
	else if (v(e)) for (let n in e) e[n] && (t += n + " ");
	return t.trim();
}
var B = "itemscope,allowfullscreen,formnovalidate,ismap,nomodule,novalidate,readonly", V = /* @__PURE__ */ e(B);
B + "";
function oe(e) {
	return !!e || e === "";
}
function se(e, t) {
	if (e.length !== t.length) return !1;
	let n = !0;
	for (let r = 0; n && r < e.length; r++) n = H(e[r], t[r]);
	return n;
}
function H(e, t) {
	if (e === t) return !0;
	let n = m(e), r = m(t);
	if (n || r) return n && r ? e.getTime() === t.getTime() : !1;
	if (n = _(e), r = _(t), n || r) return e === t;
	if (n = d(e), r = d(t), n || r) return n && r ? se(e, t) : !1;
	if (n = v(e), r = v(t), n || r) {
		if (!n || !r || Object.keys(e).length !== Object.keys(t).length) return !1;
		for (let n in e) {
			let r = e.hasOwnProperty(n), i = t.hasOwnProperty(n);
			if (r && !i || !r && i || !H(e[n], t[n])) return !1;
		}
	}
	return String(e) === String(t);
}
function ce(e, t) {
	return e.findIndex((e) => H(e, t));
}
var le = (e) => !!(e && e.__v_isRef === !0), U = (e) => g(e) ? e : e == null ? "" : d(e) || v(e) && (e.toString === b || !h(e.toString)) ? le(e) ? U(e.value) : JSON.stringify(e, ue, 2) : String(e), ue = (e, t) => le(t) ? ue(e, t.value) : f(t) ? { [`Map(${t.size})`]: [...t.entries()].reduce((e, [t, n], r) => (e[de(t, r) + " =>"] = n, e), {}) } : p(t) ? { [`Set(${t.size})`]: [...t.values()].map((e) => de(e)) } : _(t) ? de(t) : v(t) && !d(t) && !C(t) ? String(t) : t, de = (e, t = "") => _(e) ? `Symbol(${e.description ?? t})` : e, fe, pe = class {
	constructor(e = !1) {
		this.detached = e, this._active = !0, this._on = 0, this.effects = [], this.cleanups = [], this._isPaused = !1, this._warnOnRun = !0, this.__v_skip = !0, !e && fe && (fe.active ? (this.parent = fe, this.index = (fe.scopes || (fe.scopes = [])).push(this) - 1) : (this._active = !1, this._warnOnRun = !1));
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
			let t = fe;
			try {
				return fe = this, e();
			} finally {
				fe = t;
			}
		}
	}
	on() {
		++this._on === 1 && (this.prevScope = fe, fe = this);
	}
	off() {
		if (this._on > 0 && --this._on === 0) {
			if (fe === this) fe = this.prevScope;
			else {
				let e = fe;
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
function me() {
	return fe;
}
var W, he = /* @__PURE__ */ new WeakSet(), ge = class {
	constructor(e) {
		this.fn = e, this.deps = void 0, this.depsTail = void 0, this.flags = 5, this.next = void 0, this.cleanup = void 0, this.scheduler = void 0, fe && (fe.active ? fe.effects.push(this) : this.flags &= -2);
	}
	pause() {
		this.flags |= 64;
	}
	resume() {
		this.flags & 64 && (this.flags &= -65, he.has(this) && (he.delete(this), this.trigger()));
	}
	notify() {
		this.flags & 2 && !(this.flags & 32) || this.flags & 8 || be(this);
	}
	run() {
		if (!(this.flags & 1)) return this.fn();
		this.flags |= 2, Ne(this), Ce(this);
		let e = W, t = ke;
		W = this, ke = !0;
		try {
			return this.fn();
		} finally {
			we(this), W = e, ke = t, this.flags &= -3;
		}
	}
	stop() {
		if (this.flags & 1) {
			for (let e = this.deps; e; e = e.nextDep) De(e);
			this.deps = this.depsTail = void 0, Ne(this), this.onStop && this.onStop(), this.flags &= -2;
		}
	}
	trigger() {
		this.flags & 64 ? he.add(this) : this.scheduler ? this.scheduler() : this.runIfDirty();
	}
	runIfDirty() {
		Te(this) && this.run();
	}
	get dirty() {
		return Te(this);
	}
}, _e = 0, ve, ye;
function be(e, t = !1) {
	if (e.flags |= 8, t) {
		e.next = ye, ye = e;
		return;
	}
	e.next = ve, ve = e;
}
function xe() {
	_e++;
}
function Se() {
	if (--_e > 0) return;
	if (ye) {
		let e = ye;
		for (ye = void 0; e;) {
			let t = e.next;
			e.next = void 0, e.flags &= -9, e = t;
		}
	}
	let e;
	for (; ve;) {
		let t = ve;
		for (ve = void 0; t;) {
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
function Ce(e) {
	for (let t = e.deps; t; t = t.nextDep) t.version = -1, t.prevActiveLink = t.dep.activeLink, t.dep.activeLink = t;
}
function we(e) {
	let t, n = e.depsTail, r = n;
	for (; r;) {
		let e = r.prevDep;
		r.version === -1 ? (r === n && (n = e), De(r), Oe(r)) : t = r, r.dep.activeLink = r.prevActiveLink, r.prevActiveLink = void 0, r = e;
	}
	e.deps = t, e.depsTail = n;
}
function Te(e) {
	for (let t = e.deps; t; t = t.nextDep) if (t.dep.version !== t.version || t.dep.computed && (Ee(t.dep.computed) || t.dep.version !== t.version)) return !0;
	return !!e._dirty;
}
function Ee(e) {
	if (e.flags & 4 && !(e.flags & 16) || (e.flags &= -17, e.globalVersion === Pe) || (e.globalVersion = Pe, !e.isSSR && e.flags & 128 && (!e.deps && !e._dirty || !Te(e)))) return;
	e.flags |= 2;
	let t = e.dep, n = W, r = ke;
	W = e, ke = !0;
	try {
		Ce(e);
		let n = e.fn(e._value);
		(t.version === 0 || M(n, e._value)) && (e.flags |= 128, e._value = n, t.version++);
	} catch (e) {
		throw t.version++, e;
	} finally {
		W = n, ke = r, we(e), e.flags &= -3;
	}
}
function De(e, t = !1) {
	let { dep: n, prevSub: r, nextSub: i } = e;
	if (r && (r.nextSub = i, e.prevSub = void 0), i && (i.prevSub = r, e.nextSub = void 0), n.subs === e && (n.subs = r, !r && n.computed)) {
		n.computed.flags &= -5;
		for (let e = n.computed.deps; e; e = e.nextDep) De(e, !0);
	}
	!t && !--n.sc && n.map && n.map.delete(n.key);
}
function Oe(e) {
	let { prevDep: t, nextDep: n } = e;
	t && (t.nextDep = n, e.prevDep = void 0), n && (n.prevDep = t, e.nextDep = void 0);
}
var ke = !0, Ae = [];
function je() {
	Ae.push(ke), ke = !1;
}
function Me() {
	let e = Ae.pop();
	ke = e === void 0 || e;
}
function Ne(e) {
	let { cleanup: t } = e;
	if (e.cleanup = void 0, t) {
		let e = W;
		W = void 0;
		try {
			t();
		} finally {
			W = e;
		}
	}
}
var Pe = 0, Fe = class {
	constructor(e, t) {
		this.sub = e, this.dep = t, this.version = t.version, this.nextDep = this.prevDep = this.nextSub = this.prevSub = this.prevActiveLink = void 0;
	}
}, Ie = class {
	constructor(e) {
		this.computed = e, this.version = 0, this.activeLink = void 0, this.subs = void 0, this.map = void 0, this.key = void 0, this.sc = 0, this.__v_skip = !0;
	}
	track(e) {
		if (!W || !ke || W === this.computed) return;
		let t = this.activeLink;
		if (t === void 0 || t.sub !== W) t = this.activeLink = new Fe(W, this), W.deps ? (t.prevDep = W.depsTail, W.depsTail.nextDep = t, W.depsTail = t) : W.deps = W.depsTail = t, Le(t);
		else if (t.version === -1 && (t.version = this.version, t.nextDep)) {
			let e = t.nextDep;
			e.prevDep = t.prevDep, t.prevDep && (t.prevDep.nextDep = e), t.prevDep = W.depsTail, t.nextDep = void 0, W.depsTail.nextDep = t, W.depsTail = t, W.deps === t && (W.deps = e);
		}
		return t;
	}
	trigger(e) {
		this.version++, Pe++, this.notify(e);
	}
	notify(e) {
		xe();
		try {
			for (let e = this.subs; e; e = e.prevSub) e.sub.notify() && e.sub.dep.notify();
		} finally {
			Se();
		}
	}
};
function Le(e) {
	if (e.dep.sc++, e.sub.flags & 4) {
		let t = e.dep.computed;
		if (t && !e.dep.subs) {
			t.flags |= 20;
			for (let e = t.deps; e; e = e.nextDep) Le(e);
		}
		let n = e.dep.subs;
		n !== e && (e.prevSub = n, n && (n.nextSub = e)), e.dep.subs = e;
	}
}
var Re = /* @__PURE__ */ new WeakMap(), ze = /* @__PURE__ */ Symbol(""), Be = /* @__PURE__ */ Symbol(""), Ve = /* @__PURE__ */ Symbol("");
function He(e, t, n) {
	if (ke && W) {
		let t = Re.get(e);
		t || Re.set(e, t = /* @__PURE__ */ new Map());
		let r = t.get(n);
		r || (t.set(n, r = new Ie()), r.map = t, r.key = n), r.track();
	}
}
function Ue(e, t, n, r, i, a) {
	let o = Re.get(e);
	if (!o) {
		Pe++;
		return;
	}
	let s = (e) => {
		e && e.trigger();
	};
	if (xe(), t === "clear") o.forEach(s);
	else {
		let i = d(e), a = i && w(n);
		if (i && n === "length") {
			let e = Number(r);
			o.forEach((t, n) => {
				(n === "length" || n === Ve || !_(n) && n >= e) && s(t);
			});
		} else switch ((n !== void 0 || o.has(void 0)) && s(o.get(n)), a && s(o.get(Ve)), t) {
			case "add":
				i ? a && s(o.get("length")) : (s(o.get(ze)), f(e) && s(o.get(Be)));
				break;
			case "delete":
				i || (s(o.get(ze)), f(e) && s(o.get(Be)));
				break;
			case "set": f(e) && s(o.get(ze));
		}
	}
	Se();
}
function We(e) {
	let t = /* @__PURE__ */ jt(e);
	return t === e ? t : (He(t, "iterate", Ve), /* @__PURE__ */ kt(e) ? t : t.map(Nt));
}
function Ge(e) {
	return He(e = /* @__PURE__ */ jt(e), "iterate", Ve), e;
}
function Ke(e, t) {
	return /* @__PURE__ */ Ot(e) ? Pt(/* @__PURE__ */ Dt(e) ? Nt(t) : t) : Nt(t);
}
var qe = {
	__proto__: null,
	[Symbol.iterator]() {
		return Je(this, Symbol.iterator, (e) => Ke(this, e));
	},
	concat(...e) {
		return We(this).concat(...e.map((e) => d(e) ? We(e) : e));
	},
	entries() {
		return Je(this, "entries", (e) => (e[1] = Ke(this, e[1]), e));
	},
	every(e, t) {
		return Xe(this, "every", e, t, void 0, arguments);
	},
	filter(e, t) {
		return Xe(this, "filter", e, t, (e) => e.map((e) => Ke(this, e)), arguments);
	},
	find(e, t) {
		return Xe(this, "find", e, t, (e) => Ke(this, e), arguments);
	},
	findIndex(e, t) {
		return Xe(this, "findIndex", e, t, void 0, arguments);
	},
	findLast(e, t) {
		return Xe(this, "findLast", e, t, (e) => Ke(this, e), arguments);
	},
	findLastIndex(e, t) {
		return Xe(this, "findLastIndex", e, t, void 0, arguments);
	},
	forEach(e, t) {
		return Xe(this, "forEach", e, t, void 0, arguments);
	},
	includes(...e) {
		return Qe(this, "includes", e);
	},
	indexOf(...e) {
		return Qe(this, "indexOf", e);
	},
	join(e) {
		return We(this).join(e);
	},
	lastIndexOf(...e) {
		return Qe(this, "lastIndexOf", e);
	},
	map(e, t) {
		return Xe(this, "map", e, t, void 0, arguments);
	},
	pop() {
		return $e(this, "pop");
	},
	push(...e) {
		return $e(this, "push", e);
	},
	reduce(e, ...t) {
		return Ze(this, "reduce", e, t);
	},
	reduceRight(e, ...t) {
		return Ze(this, "reduceRight", e, t);
	},
	shift() {
		return $e(this, "shift");
	},
	some(e, t) {
		return Xe(this, "some", e, t, void 0, arguments);
	},
	splice(...e) {
		return $e(this, "splice", e);
	},
	toReversed() {
		return We(this).toReversed();
	},
	toSorted(e) {
		return We(this).toSorted(e);
	},
	toSpliced(...e) {
		return We(this).toSpliced(...e);
	},
	unshift(...e) {
		return $e(this, "unshift", e);
	},
	values() {
		return Je(this, "values", (e) => Ke(this, e));
	}
};
function Je(e, t, n) {
	let r = Ge(e), i = r[t]();
	return r !== e && !/* @__PURE__ */ kt(e) && (i._next = i.next, i.next = () => {
		let e = i._next();
		return e.done || (e.value = n(e.value)), e;
	}), i;
}
var Ye = Array.prototype;
function Xe(e, t, n, r, i, a) {
	let o = Ge(e), s = o !== e && !/* @__PURE__ */ kt(e), c = o[t];
	if (c !== Ye[t]) {
		let t = c.apply(e, a);
		return s ? Nt(t) : t;
	}
	let l = n;
	o !== e && (s ? l = function(t, r) {
		return n.call(this, Ke(e, t), r, e);
	} : n.length > 2 && (l = function(t, r) {
		return n.call(this, t, r, e);
	}));
	let u = c.call(o, l, r);
	return s && i ? i(u) : u;
}
function Ze(e, t, n, r) {
	let i = Ge(e), a = i !== e && !/* @__PURE__ */ kt(e), o = n, s = !1;
	i !== e && (a ? (s = r.length === 0, o = function(t, r, i) {
		return s && (s = !1, t = Ke(e, t)), n.call(this, t, Ke(e, r), i, e);
	}) : n.length > 3 && (o = function(t, r, i) {
		return n.call(this, t, r, i, e);
	}));
	let c = i[t](o, ...r);
	return s ? Ke(e, c) : c;
}
function Qe(e, t, n) {
	let r = /* @__PURE__ */ jt(e);
	He(r, "iterate", Ve);
	let i = r[t](...n);
	return (i === -1 || i === !1) && /* @__PURE__ */ At(n[0]) ? (n[0] = /* @__PURE__ */ jt(n[0]), r[t](...n)) : i;
}
function $e(e, t, n = []) {
	je(), xe();
	let r = (/* @__PURE__ */ jt(e))[t].apply(e, n);
	return Se(), Me(), r;
}
var et = /* @__PURE__ */ e("__proto__,__v_isRef,__isVue"), tt = new Set(/* @__PURE__ */ Object.getOwnPropertyNames(Symbol).filter((e) => e !== "arguments" && e !== "caller").map((e) => Symbol[e]).filter(_));
function nt(e) {
	_(e) || (e = String(e));
	let t = /* @__PURE__ */ jt(this);
	return He(t, "has", e), t.hasOwnProperty(e);
}
var rt = class {
	constructor(e = !1, t = !1) {
		this._isReadonly = e, this._isShallow = t;
	}
	get(e, t, n) {
		if (t === "__v_skip") return e.__v_skip;
		let r = this._isReadonly, i = this._isShallow;
		if (t === "__v_isReactive") return !r;
		if (t === "__v_isReadonly") return r;
		if (t === "__v_isShallow") return i;
		if (t === "__v_raw") return n === (r ? i ? xt : bt : i ? yt : vt).get(e) || Object.getPrototypeOf(e) === Object.getPrototypeOf(n) ? e : void 0;
		let a = d(e);
		if (!r) {
			let e;
			if (a && (e = qe[t])) return e;
			if (t === "hasOwnProperty") return nt;
		}
		let o = Reflect.get(e, t, /* @__PURE__ */ Ft(e) ? e : n);
		if ((_(t) ? tt.has(t) : et(t)) || (r || He(e, "get", t), i)) return o;
		if (/* @__PURE__ */ Ft(o)) {
			let e = a && w(t) ? o : o.value;
			return r && v(e) ? /* @__PURE__ */ Tt(e) : e;
		}
		return v(o) ? r ? /* @__PURE__ */ Tt(o) : /* @__PURE__ */ Ct(o) : o;
	}
}, it = class extends rt {
	constructor(e = !1) {
		super(!1, e);
	}
	set(e, t, n, r) {
		let i = e[t], a = d(e) && w(t);
		if (!this._isShallow) {
			let e = /* @__PURE__ */ Ot(i);
			if (!/* @__PURE__ */ kt(n) && !/* @__PURE__ */ Ot(n) && (i = /* @__PURE__ */ jt(i), n = /* @__PURE__ */ jt(n)), !a && /* @__PURE__ */ Ft(i) && !/* @__PURE__ */ Ft(n)) return e || (i.value = n), !0;
		}
		let o = a ? Number(t) < e.length : u(e, t), s = Reflect.set(e, t, n, /* @__PURE__ */ Ft(e) ? e : r);
		return e === /* @__PURE__ */ jt(r) && s && (o ? M(n, i) && Ue(e, "set", t, n, i) : Ue(e, "add", t, n)), s;
	}
	deleteProperty(e, t) {
		let n = u(e, t), r = e[t], i = Reflect.deleteProperty(e, t);
		return i && n && Ue(e, "delete", t, void 0, r), i;
	}
	has(e, t) {
		let n = Reflect.has(e, t);
		return (!_(t) || !tt.has(t)) && He(e, "has", t), n;
	}
	ownKeys(e) {
		return He(e, "iterate", d(e) ? "length" : ze), Reflect.ownKeys(e);
	}
}, at = class extends rt {
	constructor(e = !1) {
		super(!0, e);
	}
	set(e, t) {
		return !0;
	}
	deleteProperty(e, t) {
		return !0;
	}
}, ot = /* @__PURE__ */ new it(), st = /* @__PURE__ */ new at(), ct = /* @__PURE__ */ new it(!0), lt = (e) => e, ut = (e) => Reflect.getPrototypeOf(e);
function dt(e, t, n) {
	return function(...r) {
		let i = this.__v_raw, a = /* @__PURE__ */ jt(i), o = f(a), c = e === "entries" || e === Symbol.iterator && o, l = e === "keys" && o, u = i[e](...r), d = n ? lt : t ? Pt : Nt;
		return !t && He(a, "iterate", l ? Be : ze), s(Object.create(u), { next() {
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
function ft(e) {
	return function(...t) {
		return e === "delete" ? !1 : e === "clear" ? void 0 : this;
	};
}
function pt(e, t) {
	let n = {
		get(n) {
			let r = this.__v_raw, i = /* @__PURE__ */ jt(r), a = /* @__PURE__ */ jt(n);
			e || (M(n, a) && He(i, "get", n), He(i, "get", a));
			let { has: o } = ut(i), s = t ? lt : e ? Pt : Nt;
			if (o.call(i, n)) return s(r.get(n));
			if (o.call(i, a)) return s(r.get(a));
			r !== i && r.get(n);
		},
		get size() {
			let t = this.__v_raw;
			return !e && He(/* @__PURE__ */ jt(t), "iterate", ze), t.size;
		},
		has(t) {
			let n = this.__v_raw, r = /* @__PURE__ */ jt(n), i = /* @__PURE__ */ jt(t);
			return e || (M(t, i) && He(r, "has", t), He(r, "has", i)), t === i ? n.has(t) : n.has(t) || n.has(i);
		},
		forEach(n, r) {
			let i = this, a = i.__v_raw, o = /* @__PURE__ */ jt(a), s = t ? lt : e ? Pt : Nt;
			return !e && He(o, "iterate", ze), a.forEach((e, t) => n.call(r, s(e), s(t), i));
		}
	};
	return s(n, e ? {
		add: ft("add"),
		set: ft("set"),
		delete: ft("delete"),
		clear: ft("clear")
	} : {
		add(e) {
			let n = /* @__PURE__ */ jt(this), r = ut(n), i = /* @__PURE__ */ jt(e), a = !t && !/* @__PURE__ */ kt(e) && !/* @__PURE__ */ Ot(e) ? i : e;
			return r.has.call(n, a) || M(e, a) && r.has.call(n, e) || M(i, a) && r.has.call(n, i) || (n.add(a), Ue(n, "add", a, a)), this;
		},
		set(e, n) {
			!t && !/* @__PURE__ */ kt(n) && !/* @__PURE__ */ Ot(n) && (n = /* @__PURE__ */ jt(n));
			let r = /* @__PURE__ */ jt(this), { has: i, get: a } = ut(r), o = i.call(r, e);
			o ||= (e = /* @__PURE__ */ jt(e), i.call(r, e));
			let s = a.call(r, e);
			return r.set(e, n), o ? M(n, s) && Ue(r, "set", e, n, s) : Ue(r, "add", e, n), this;
		},
		delete(e) {
			let t = /* @__PURE__ */ jt(this), { has: n, get: r } = ut(t), i = n.call(t, e);
			i ||= (e = /* @__PURE__ */ jt(e), n.call(t, e));
			let a = r ? r.call(t, e) : void 0, o = t.delete(e);
			return i && Ue(t, "delete", e, void 0, a), o;
		},
		clear() {
			let e = /* @__PURE__ */ jt(this), t = e.size !== 0, n = e.clear();
			return t && Ue(e, "clear", void 0, void 0, void 0), n;
		}
	}), [
		"keys",
		"values",
		"entries",
		Symbol.iterator
	].forEach((r) => {
		n[r] = dt(r, e, t);
	}), n;
}
function mt(e, t) {
	let n = pt(e, t);
	return (t, r, i) => r === "__v_isReactive" ? !e : r === "__v_isReadonly" ? e : r === "__v_raw" ? t : Reflect.get(u(n, r) && r in t ? n : t, r, i);
}
var ht = { get: /* @__PURE__ */ mt(!1, !1) }, gt = { get: /* @__PURE__ */ mt(!1, !0) }, _t = { get: /* @__PURE__ */ mt(!0, !1) }, vt = /* @__PURE__ */ new WeakMap(), yt = /* @__PURE__ */ new WeakMap(), bt = /* @__PURE__ */ new WeakMap(), xt = /* @__PURE__ */ new WeakMap();
function St(e) {
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
function Ct(e) {
	return /* @__PURE__ */ Ot(e) ? e : Et(e, !1, ot, ht, vt);
}
// @__NO_SIDE_EFFECTS__
function wt(e) {
	return Et(e, !1, ct, gt, yt);
}
// @__NO_SIDE_EFFECTS__
function Tt(e) {
	return Et(e, !0, st, _t, bt);
}
function Et(e, t, n, r, i) {
	if (!v(e) || e.__v_raw && !(t && e.__v_isReactive) || e.__v_skip || !Object.isExtensible(e)) return e;
	let a = i.get(e);
	if (a) return a;
	let o = St(S(e));
	if (o === 0) return e;
	let s = new Proxy(e, o === 2 ? r : n);
	return i.set(e, s), s;
}
// @__NO_SIDE_EFFECTS__
function Dt(e) {
	return /* @__PURE__ */ Ot(e) ? /* @__PURE__ */ Dt(e.__v_raw) : !!(e && e.__v_isReactive);
}
// @__NO_SIDE_EFFECTS__
function Ot(e) {
	return !!(e && e.__v_isReadonly);
}
// @__NO_SIDE_EFFECTS__
function kt(e) {
	return !!(e && e.__v_isShallow);
}
// @__NO_SIDE_EFFECTS__
function At(e) {
	return e ? !!e.__v_raw : !1;
}
// @__NO_SIDE_EFFECTS__
function jt(e) {
	let t = e && e.__v_raw;
	return t ? /* @__PURE__ */ jt(t) : e;
}
function Mt(e) {
	return !u(e, "__v_skip") && Object.isExtensible(e) && N(e, "__v_skip", !0), e;
}
var Nt = (e) => v(e) ? /* @__PURE__ */ Ct(e) : e, Pt = (e) => v(e) ? /* @__PURE__ */ Tt(e) : e;
// @__NO_SIDE_EFFECTS__
function Ft(e) {
	return e ? e.__v_isRef === !0 : !1;
}
// @__NO_SIDE_EFFECTS__
function G(e) {
	return It(e, !1);
}
function It(e, t) {
	return /* @__PURE__ */ Ft(e) ? e : new Lt(e, t);
}
var Lt = class {
	constructor(e, t) {
		this.dep = new Ie(), this.__v_isRef = !0, this.__v_isShallow = !1, this._rawValue = t ? e : /* @__PURE__ */ jt(e), this._value = t ? e : Nt(e), this.__v_isShallow = t;
	}
	get value() {
		return this.dep.track(), this._value;
	}
	set value(e) {
		let t = this._rawValue, n = this.__v_isShallow || /* @__PURE__ */ kt(e) || /* @__PURE__ */ Ot(e);
		e = n ? e : /* @__PURE__ */ jt(e), M(e, t) && (this._rawValue = e, this._value = n ? e : Nt(e), this.dep.trigger());
	}
};
function Rt(e) {
	return /* @__PURE__ */ Ft(e) ? e.value : e;
}
var zt = {
	get: (e, t, n) => t === "__v_raw" ? e : Rt(Reflect.get(e, t, n)),
	set: (e, t, n, r) => {
		let i = e[t];
		return /* @__PURE__ */ Ft(i) && !/* @__PURE__ */ Ft(n) ? (i.value = n, !0) : Reflect.set(e, t, n, r);
	}
};
function Bt(e) {
	return /* @__PURE__ */ Dt(e) ? e : new Proxy(e, zt);
}
var Vt = class {
	constructor(e, t, n) {
		this.fn = e, this.setter = t, this._value = void 0, this.dep = new Ie(this), this.__v_isRef = !0, this.deps = void 0, this.depsTail = void 0, this.flags = 16, this.globalVersion = Pe - 1, this.next = void 0, this.effect = this, this.__v_isReadonly = !t, this.isSSR = n;
	}
	notify() {
		if (this.flags |= 16, !(this.flags & 8) && W !== this) return be(this, !0), !0;
	}
	get value() {
		let e = this.dep.track();
		return Ee(this), e && (e.version = this.dep.version), this._value;
	}
	set value(e) {
		this.setter && this.setter(e);
	}
};
// @__NO_SIDE_EFFECTS__
function Ht(e, t, n = !1) {
	let r, i;
	return h(e) ? r = e : (r = e.get, i = e.set), new Vt(r, i, n);
}
var Ut = {}, Wt = /* @__PURE__ */ new WeakMap(), Gt = void 0;
function Kt(e, t = !1, n = Gt) {
	if (n) {
		let t = Wt.get(n);
		t || Wt.set(n, t = []), t.push(e);
	}
}
function qt(e, n, i = t) {
	let { immediate: a, deep: o, once: s, scheduler: l, augmentJob: u, call: f } = i, p = (e) => o ? e : /* @__PURE__ */ kt(e) || o === !1 || o === 0 ? Jt(e, 1) : Jt(e), m, g, _, v, y = !1, b = !1;
	if (/* @__PURE__ */ Ft(e) ? (g = () => e.value, y = /* @__PURE__ */ kt(e)) : /* @__PURE__ */ Dt(e) ? (g = () => p(e), y = !0) : d(e) ? (b = !0, y = e.some((e) => /* @__PURE__ */ Dt(e) || /* @__PURE__ */ kt(e)), g = () => e.map((e) => {
		if (/* @__PURE__ */ Ft(e)) return e.value;
		if (/* @__PURE__ */ Dt(e)) return p(e);
		if (h(e)) return f ? f(e, 2) : e();
	})) : g = h(e) ? n ? f ? () => f(e, 2) : e : () => {
		if (_) {
			je();
			try {
				_();
			} finally {
				Me();
			}
		}
		let t = Gt;
		Gt = m;
		try {
			return f ? f(e, 3, [v]) : e(v);
		} finally {
			Gt = t;
		}
	} : r, n && o) {
		let e = g, t = o === !0 ? Infinity : o;
		g = () => Jt(e(), t);
	}
	let x = me(), S = () => {
		m.stop(), x && x.active && c(x.effects, m);
	};
	if (s && n) {
		let e = n;
		n = (...t) => {
			let n = e(...t);
			return S(), n;
		};
	}
	let C = b ? Array(e.length).fill(Ut) : Ut, w = (e) => {
		if (!(!(m.flags & 1) || !m.dirty && !e)) if (n) {
			let t = m.run();
			if (e || o || y || (b ? t.some((e, t) => M(e, C[t])) : M(t, C))) {
				_ && _();
				let e = Gt;
				Gt = m;
				try {
					let e = [
						t,
						C === Ut ? void 0 : b && C[0] === Ut ? [] : C,
						v
					];
					C = t, f ? f(n, 3, e) : n(...e);
				} finally {
					Gt = e;
				}
			}
		} else m.run();
	};
	return u && u(w), m = new ge(g), m.scheduler = l ? () => l(w, !1) : w, v = (e) => Kt(e, !1, m), _ = m.onStop = () => {
		let e = Wt.get(m);
		if (e) {
			if (f) f(e, 4);
			else for (let t of e) t();
			Wt.delete(m);
		}
	}, n ? a ? w(!0) : C = m.run() : l ? l(w.bind(null, !0), !0) : m.run(), S.pause = m.pause.bind(m), S.resume = m.resume.bind(m), S.stop = S, S;
}
function Jt(e, t = Infinity, n) {
	if (t <= 0 || !v(e) || e.__v_skip || (n ||= /* @__PURE__ */ new Map(), (n.get(e) || 0) >= t)) return e;
	if (n.set(e, t), t--, /* @__PURE__ */ Ft(e)) Jt(e.value, t, n);
	else if (d(e)) for (let r = 0; r < e.length; r++) Jt(e[r], t, n);
	else if (p(e) || f(e)) e.forEach((e) => {
		Jt(e, t, n);
	});
	else if (C(e)) {
		for (let r in e) Jt(e[r], t, n);
		for (let r of Object.getOwnPropertySymbols(e)) Object.prototype.propertyIsEnumerable.call(e, r) && Jt(e[r], t, n);
	}
	return e;
}
//#endregion
//#region node_modules/@vue/runtime-core/dist/runtime-core.esm-bundler.js
function Yt(e, t, n, r) {
	try {
		return r ? e(...r) : e();
	} catch (e) {
		Zt(e, t, n);
	}
}
function Xt(e, t, n, r) {
	if (h(e)) {
		let i = Yt(e, t, n, r);
		return i && y(i) && i.catch((e) => {
			Zt(e, t, n);
		}), i;
	}
	if (d(e)) {
		let i = [];
		for (let a = 0; a < e.length; a++) i.push(Xt(e[a], t, n, r));
		return i;
	}
}
function Zt(e, n, r, i = !0) {
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
			je(), Yt(o, null, 10, [
				e,
				i,
				a
			]), Me();
			return;
		}
	}
	Qt(e, r, a, i, s);
}
function Qt(e, t, n, r = !0, i = !1) {
	if (i) throw e;
	console.error(e);
}
var $t = [], en = -1, tn = [], nn = null, rn = 0, an = /* @__PURE__ */ Promise.resolve(), on = null;
function sn(e) {
	let t = on || an;
	return e ? t.then(this ? e.bind(this) : e) : t;
}
function cn(e) {
	let t = en + 1, n = $t.length;
	for (; t < n;) {
		let r = t + n >>> 1, i = $t[r], a = mn(i);
		a < e || a === e && i.flags & 2 ? t = r + 1 : n = r;
	}
	return t;
}
function ln(e) {
	if (!(e.flags & 1)) {
		let t = mn(e), n = $t[$t.length - 1];
		!n || !(e.flags & 2) && t >= mn(n) ? $t.push(e) : $t.splice(cn(t), 0, e), e.flags |= 1, un();
	}
}
function un() {
	on ||= an.then(hn);
}
function dn(e) {
	d(e) ? tn.push(...e) : nn && e.id === -1 ? nn.splice(rn + 1, 0, e) : e.flags & 1 || (tn.push(e), e.flags |= 1), un();
}
function fn(e, t, n = en + 1) {
	for (; n < $t.length; n++) {
		let t = $t[n];
		if (t && t.flags & 2) {
			if (e && t.id !== e.uid) continue;
			$t.splice(n, 1), n--, t.flags & 4 && (t.flags &= -2), t(), t.flags & 4 || (t.flags &= -2);
		}
	}
}
function pn(e) {
	if (tn.length) {
		let e = [...new Set(tn)].sort((e, t) => mn(e) - mn(t));
		if (tn.length = 0, nn) {
			nn.push(...e);
			return;
		}
		for (nn = e, rn = 0; rn < nn.length; rn++) {
			let e = nn[rn];
			e.flags & 4 && (e.flags &= -2), e.flags & 8 || e(), e.flags &= -2;
		}
		nn = null, rn = 0;
	}
}
var mn = (e) => e.id == null ? e.flags & 2 ? -1 : Infinity : e.id;
function hn(e) {
	try {
		for (en = 0; en < $t.length; en++) {
			let e = $t[en];
			e && !(e.flags & 8) && (e.flags & 4 && (e.flags &= -2), Yt(e, e.i, e.i ? 15 : 14), e.flags & 4 || (e.flags &= -2));
		}
	} finally {
		for (; en < $t.length; en++) {
			let e = $t[en];
			e && (e.flags &= -2);
		}
		en = -1, $t.length = 0, pn(e), on = null, ($t.length || tn.length) && hn(e);
	}
}
var gn = null, _n = null;
function vn(e) {
	let t = gn;
	return gn = e, _n = e && e.type.__scopeId || null, t;
}
function yn(e, t = gn, n) {
	if (!t || e._n) return e;
	let r = (...n) => {
		r._d && Qi(-1);
		let i = vn(t), a = Ji.length, o;
		try {
			o = e(...n);
		} finally {
			for (let e = Ji.length; e > a; e--) Xi();
			vn(i), r._d && Qi(1);
		}
		return o;
	};
	return r._n = !0, r._c = !0, r._d = !0, r;
}
function bn(e, n) {
	if (gn === null) return e;
	let r = Na(gn), i = e.dirs ||= [];
	for (let e = 0; e < n.length; e++) {
		let [a, o, s, c = t] = n[e];
		a && (h(a) && (a = {
			mounted: a,
			updated: a
		}), a.deep && Jt(o), i.push({
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
function xn(e, t, n, r) {
	let i = e.dirs, a = t && t.dirs;
	for (let o = 0; o < i.length; o++) {
		let s = i[o];
		a && (s.oldValue = a[o].value);
		let c = s.dir[r];
		c && (je(), Xt(c, n, 8, [
			e.el,
			s,
			e,
			t
		]), Me());
	}
}
function Sn(e, t) {
	if (_a) {
		let n = _a.provides, r = _a.parent && _a.parent.provides;
		r === n && (n = _a.provides = Object.create(r)), n[e] = t;
	}
}
function Cn(e, t, n = !1) {
	let r = va();
	if (r || ti) {
		let i = ti ? ti._context.provides : r ? r.parent == null || r.ce ? r.vnode.appContext && r.vnode.appContext.provides : r.parent.provides : void 0;
		if (i && e in i) return i[e];
		if (arguments.length > 1) return n && h(t) ? t.call(r && r.proxy) : t;
	}
}
var wn = /* @__PURE__ */ Symbol.for("v-scx"), Tn = () => Cn(wn);
function En(e, t, n) {
	return Dn(e, t, n);
}
function Dn(e, n, i = t) {
	let { immediate: a, deep: o, flush: c, once: l } = i, u = s({}, i), d = n && a || !n && c !== "post", f;
	if (wa) {
		if (c === "sync") {
			let e = Tn();
			f = e.__watcherHandles ||= [];
		} else if (!d) {
			let e = () => {};
			return e.stop = r, e.resume = r, e.pause = r, e;
		}
	}
	let p = _a;
	u.call = (e, t, n) => Xt(e, p, t, n);
	let m = !1;
	c === "post" ? u.scheduler = (e) => {
		Mi(e, p && p.suspense);
	} : c !== "sync" && (m = !0, u.scheduler = (e, t) => {
		t ? e() : ln(e);
	}), u.augmentJob = (e) => {
		n && (e.flags |= 4), m && (e.flags |= 2, p && (e.id = p.uid, e.i = p));
	};
	let h = qt(e, n, u);
	return wa && (f ? f.push(h) : d && h()), h;
}
function On(e, t, n) {
	let r = this.proxy, i = g(e) ? e.includes(".") ? kn(r, e) : () => r[e] : e.bind(r, r), a;
	h(t) ? a = t : (a = t.handler, n = t);
	let o = xa(this), s = Dn(i, a.bind(r), n);
	return o(), s;
}
function kn(e, t) {
	let n = t.split(".");
	return () => {
		let t = e;
		for (let e = 0; e < n.length && t; e++) t = t[n[e]];
		return t;
	};
}
var An = /* @__PURE__ */ new WeakMap(), jn = /* @__PURE__ */ Symbol("_vte"), Mn = (e) => e.__isTeleport, Nn = (e) => e && (e.disabled || e.disabled === ""), Pn = (e) => e && (e.defer || e.defer === ""), Fn = (e) => typeof SVGElement < "u" && e instanceof SVGElement, In = (e) => typeof MathMLElement == "function" && e instanceof MathMLElement, Ln = (e, t) => {
	let n = e && e.to;
	return g(n) ? t ? t(n) : null : n;
}, Rn = {
	name: "Teleport",
	__isTeleport: !0,
	process(e, t, n, r, i, a, o, s, c, l) {
		let { mc: u, pc: d, pbc: f, o: { insert: p, querySelector: m, createText: h, createComment: g, parentNode: _ } } = l, v = Nn(t.props), { dynamicChildren: y } = t, b = (e, t, n) => {
			e.shapeFlag & 16 && u(e.children, t, n, i, a, o, s, c);
		}, x = (e = t) => {
			let n = Nn(e.props), r = e.target = Ln(e.props, m), a = Un(r, e, h, p);
			r && (o !== "svg" && Fn(r) ? o = "svg" : o !== "mathml" && In(r) && (o = "mathml"), i && i.isCE && (i.ce._teleportTargets || (i.ce._teleportTargets = /* @__PURE__ */ new Set())).add(r), n || (b(e, r, a), Hn(e, !1)));
		}, S = (e) => {
			let t = () => {
				if (An.get(e) === t) {
					if (An.delete(e), Nn(e.props)) {
						let t = _(e.el) || n;
						b(e, t, e.anchor), Hn(e, !0);
					}
					x(e);
				}
			};
			An.set(e, t), Mi(t, a);
		};
		if (e == null) {
			let e = t.el = h(""), i = t.anchor = h("");
			if (p(e, n, r), p(i, n, r), Pn(t.props) || a && a.pendingBranch) {
				S(t);
				return;
			}
			v && (b(t, n, i), Hn(t, !0)), x();
		} else {
			t.el = e.el;
			let r = t.anchor = e.anchor, u = An.get(e);
			if (u) {
				u.flags |= 8, An.delete(e), S(t);
				return;
			}
			t.targetStart = e.targetStart;
			let p = t.target = e.target, h = t.targetAnchor = e.targetAnchor, g = Nn(e.props), _ = g ? n : p, b = g ? r : h;
			if (o === "svg" || Fn(p) ? o = "svg" : (o === "mathml" || In(p)) && (o = "mathml"), y ? (f(e.dynamicChildren, y, _, i, a, o, s), Ri(e, t, !0)) : c || d(e, t, _, b, i, a, o, s, !1), v) g ? t.props && e.props && t.props.to !== e.props.to && (t.props.to = e.props.to) : zn(t, n, r, l, 1);
			else if ((t.props && t.props.to) !== (e.props && e.props.to)) {
				let e = Ln(t.props, m);
				e && (t.target = e, zn(t, e, null, l, 0));
			} else g && zn(t, p, h, l, 1);
			Hn(t, v);
		}
	},
	remove(e, t, n, { um: r, o: { remove: i } }, a) {
		let { shapeFlag: o, children: s, anchor: c, targetStart: l, targetAnchor: u, target: d, props: f } = e, p = Nn(f), m = a || !p, h = An.get(e);
		if (h && (h.flags |= 8, An.delete(e)), d && (i(l), i(u)), a && i(c), !h && (p || d) && o & 16) for (let e = 0; e < s.length; e++) {
			let i = s[e];
			r(i, t, n, m, !!i.dynamicChildren);
		}
	},
	move: zn,
	hydrate: Bn
};
function zn(e, t, n, { o: { insert: r }, m: i }, a = 2) {
	a === 0 && r(e.targetAnchor, t, n);
	let { el: o, anchor: s, shapeFlag: c, children: l, props: u } = e, d = a === 2;
	if (d && r(o, t, n), !An.has(e) && (!d || Nn(u)) && c & 16) for (let e = 0; e < l.length; e++) i(l[e], t, n, 2);
	d && r(s, t, n);
}
function Bn(e, t, n, r, i, a, { o: { nextSibling: o, parentNode: s, querySelector: c, insert: l, createText: u } }, d) {
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
	let m = t.target = Ln(t.props, c), h = Nn(t.props);
	if (m) {
		let c = m._lpa || m.firstChild;
		t.shapeFlag & 16 && (h ? (p(e, t), f(m, c), t.targetAnchor || Un(m, t, u, l, s(e) === m ? e : null)) : (t.anchor = o(e), f(m, c), t.targetAnchor || Un(m, t, u, l), d(c && o(c), t, m, n, r, i, a))), Hn(t, h);
	} else h && t.shapeFlag & 16 && (p(e, t), t.targetStart = e, t.targetAnchor = o(e));
	return t.anchor && o(t.anchor);
}
var Vn = Rn;
function Hn(e, t) {
	let n = e.ctx;
	if (n && n.ut) {
		let r, i;
		for (t ? (r = e.el, i = e.anchor) : (r = e.targetStart, i = e.targetAnchor); r && r !== i;) r.nodeType === 1 && r.setAttribute("data-v-owner", n.uid), r = r.nextSibling;
		n.ut();
	}
}
function Un(e, t, n, r, i = null) {
	let a = t.targetStart = n(""), o = t.targetAnchor = n("");
	return a[jn] = o, e && (r(a, e, i), r(o, e, i)), o;
}
var Wn = /* @__PURE__ */ Symbol("_leaveCb"), Gn = /* @__PURE__ */ Symbol("_enterCb");
function Kn() {
	let e = {
		isMounted: !1,
		isLeaving: !1,
		isUnmounting: !1,
		leavingVNodes: /* @__PURE__ */ new Map()
	};
	return br(() => {
		e.isMounted = !0;
	}), Cr(() => {
		e.isUnmounting = !0;
	}), e;
}
var qn = [Function, Array], Jn = {
	mode: String,
	appear: Boolean,
	persisted: Boolean,
	onBeforeEnter: qn,
	onEnter: qn,
	onAfterEnter: qn,
	onEnterCancelled: qn,
	onBeforeLeave: qn,
	onLeave: qn,
	onAfterLeave: qn,
	onLeaveCancelled: qn,
	onBeforeAppear: qn,
	onAppear: qn,
	onAfterAppear: qn,
	onAppearCancelled: qn
}, Yn = (e) => {
	let t = e.subTree;
	return t.component ? Yn(t.component) : t;
}, Xn = {
	name: "BaseTransition",
	props: Jn,
	setup(e, { slots: t }) {
		let n = va(), r = Kn();
		return () => {
			let i = t.default && ir(t.default(), !0), a = i && i.length ? Zn(i) : n.subTree ? Q() : void 0;
			if (!a) return;
			let o = /* @__PURE__ */ jt(e), { mode: s } = o;
			if (r.isLeaving) return tr(a);
			let c = nr(a);
			if (!c) return tr(a);
			let l = er(c, o, r, n, (e) => l = e);
			c.type !== Ki && rr(c, l);
			let u = n.subTree && nr(n.subTree);
			if (u && u.type !== Ki && !na(u, c) && Yn(n).type !== Ki) {
				let e = er(u, o, r, n);
				if (rr(u, e), s === "out-in" && c.type !== Ki) return r.isLeaving = !0, e.afterLeave = () => {
					r.isLeaving = !1, n.job.flags & 8 || n.update(), delete e.afterLeave, u = void 0;
				}, tr(a);
				s === "in-out" && c.type !== Ki ? e.delayLeave = (e, t, n) => {
					let i = $n(r, u);
					i[String(u.key)] = u, e[Wn] = () => {
						t(), e[Wn] = void 0, delete l.delayedLeave, u = void 0;
					}, l.delayedLeave = () => {
						n(), delete l.delayedLeave, u = void 0;
					};
				} : u = void 0;
			} else u &&= void 0;
			return a;
		};
	}
};
function Zn(e) {
	let t = e[0];
	if (e.length > 1) {
		for (let n of e) if (n.type !== Ki) {
			t = n;
			break;
		}
	}
	return t;
}
var Qn = Xn;
function $n(e, t) {
	let { leavingVNodes: n } = e, r = n.get(t.type);
	return r || (r = /* @__PURE__ */ Object.create(null), n.set(t.type, r)), r;
}
function er(e, t, n, r, i) {
	let { appear: a, mode: o, persisted: s = !1, onBeforeEnter: c, onEnter: l, onAfterEnter: u, onEnterCancelled: f, onBeforeLeave: p, onLeave: m, onAfterLeave: h, onLeaveCancelled: g, onBeforeAppear: _, onAppear: v, onAfterAppear: y, onAppearCancelled: b } = t, x = String(e.key), S = $n(n, e), C = (e, t) => {
		e && Xt(e, r, 9, t);
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
			t[Wn] && t[Wn](!0);
			let i = S[x];
			i && na(e, i) && i.el[Wn] && i.el[Wn](), C(r, [t]);
		},
		enter(t) {
			if (S[x] === e) return;
			let r = l, i = u, o = f;
			if (!n.isMounted) if (a) r = v || l, i = y || u, o = b || f;
			else return;
			let s = !1;
			t[Gn] = (e) => {
				s || (s = !0, C(e ? o : i, [t]), T.delayedLeave && T.delayedLeave(), t[Gn] = void 0);
			};
			let c = t[Gn].bind(null, !1);
			r ? w(r, [t, c]) : c();
		},
		leave(t, r) {
			let i = String(e.key);
			if (t[Gn] && t[Gn](!0), n.isUnmounting) return r();
			C(p, [t]);
			let a = !1;
			t[Wn] = (n) => {
				a || (a = !0, r(), C(n ? g : h, [t]), t[Wn] = void 0, S[i] === e && delete S[i]);
			};
			let o = t[Wn].bind(null, !1);
			S[i] = e, m ? w(m, [t, o]) : o();
		},
		clone(e) {
			let a = er(e, t, n, r, i);
			return i && i(a), a;
		}
	};
	return T;
}
function tr(e) {
	if (fr(e)) return e = ca(e), e.children = null, e;
}
function nr(e) {
	if (!fr(e)) return Mn(e.type) && e.children ? Zn(e.children) : e;
	if (e.component) return e.component.subTree;
	let { shapeFlag: t, children: n } = e;
	if (n) {
		if (t & 16) return n[0];
		if (t & 32 && h(n.default)) return n.default();
	}
}
function rr(e, t) {
	e.shapeFlag & 6 && e.component ? (e.transition = t, rr(e.component.subTree, t)) : e.shapeFlag & 128 ? (e.ssContent.transition = t.clone(e.ssContent), e.ssFallback.transition = t.clone(e.ssFallback)) : e.transition = t;
}
function ir(e, t = !1, n) {
	let r = [], i = 0;
	for (let a = 0; a < e.length; a++) {
		let o = e[a], s = n == null ? o.key : String(n) + String(o.key == null ? a : o.key);
		o.type === q ? (o.patchFlag & 128 && i++, r = r.concat(ir(o.children, t, s))) : (t || o.type !== Ki) && r.push(s == null ? o : ca(o, { key: s }));
	}
	if (i > 1) for (let e = 0; e < r.length; e++) r[e].patchFlag = -2;
	return r;
}
// @__NO_SIDE_EFFECTS__
function ar(e, t) {
	return h(e) ? /* @__PURE__ */ s({ name: e.name }, t, { setup: e }) : e;
}
function or(e) {
	e.ids = [
		e.ids[0] + e.ids[2]++ + "-",
		0,
		0
	];
}
function sr(e, t) {
	let n;
	return !!((n = Object.getOwnPropertyDescriptor(e, t)) && !n.configurable);
}
var cr = /* @__PURE__ */ new WeakMap();
function lr(e, n, r, a, o = !1) {
	if (d(e)) {
		e.forEach((e, t) => lr(e, n && (d(n) ? n[t] : n), r, a, o));
		return;
	}
	if (dr(a) && !o) {
		a.shapeFlag & 512 && a.type.__asyncResolved && a.component.subTree.component && lr(e, n, r, a.component.subTree);
		return;
	}
	let s = a.shapeFlag & 4 ? Na(a.component) : a.el, l = o ? null : s, { i: f, r: p } = e, m = n && n.r, _ = f.refs === t ? f.refs = {} : f.refs, v = f.setupState, y = /* @__PURE__ */ jt(v), b = v === t ? i : (e) => !sr(_, e) && u(y, e), x = (e, t) => !(t && sr(_, t));
	if (m != null && m !== p) {
		if (ur(n), g(m)) _[m] = null, b(m) && (v[m] = null);
		else if (/* @__PURE__ */ Ft(m)) {
			let e = n;
			x(m, e.k) && (m.value = null), e.k && (_[e.k] = null);
		}
	}
	if (h(p)) Yt(p, f, 12, [l, _]);
	else {
		let t = g(p), n = /* @__PURE__ */ Ft(p);
		if (t || n) {
			let i = () => {
				if (e.f) {
					let n = t ? b(p) ? v[p] : _[p] : x(p) || !e.k ? p.value : _[e.k];
					if (o) d(n) && c(n, s);
					else if (d(n)) n.includes(s) || n.push(s);
					else if (t) _[p] = [s], b(p) && (v[p] = _[p]);
					else {
						let t = [s];
						x(p, e.k) && (p.value = t), e.k && (_[e.k] = t);
					}
				} else t ? (_[p] = l, b(p) && (v[p] = l)) : n && (x(p, e.k) && (p.value = l), e.k && (_[e.k] = l));
			};
			if (l) {
				let t = () => {
					i(), cr.delete(e);
				};
				t.id = -1, cr.set(e, t), Mi(t, r);
			} else ur(e), i();
		}
	}
}
function ur(e) {
	let t = cr.get(e);
	t && (t.flags |= 8, cr.delete(e));
}
F().requestIdleCallback, F().cancelIdleCallback;
var dr = (e) => !!e.type.__asyncLoader, fr = (e) => e.type.__isKeepAlive;
function pr(e, t) {
	hr(e, "a", t);
}
function mr(e, t) {
	hr(e, "da", t);
}
function hr(e, t, n = _a) {
	let r = e.__wdc ||= () => {
		let t = n;
		for (; t;) {
			if (t.isDeactivated) return;
			t = t.parent;
		}
		return e();
	};
	if (_r(t, r, n), n) {
		let e = n.parent;
		for (; e && e.parent;) fr(e.parent.vnode) && gr(r, t, n, e), e = e.parent;
	}
}
function gr(e, t, n, r) {
	let i = _r(t, e, r, !0);
	wr(() => {
		c(r[t], i);
	}, n);
}
function _r(e, t, n = _a, r = !1) {
	if (n) {
		let i = n[e] || (n[e] = []), a = t.__weh ||= (...r) => {
			je();
			let i = xa(n), a = Xt(t, n, e, r);
			return i(), Me(), a;
		};
		return r ? i.unshift(a) : i.push(a), a;
	}
}
var vr = (e) => (t, n = _a) => {
	(!wa || e === "sp") && _r(e, (...e) => t(...e), n);
}, yr = vr("bm"), br = vr("m"), xr = vr("bu"), Sr = vr("u"), Cr = vr("bum"), wr = vr("um"), Tr = vr("sp"), Er = vr("rtg"), Dr = vr("rtc");
function Or(e, t = _a) {
	_r("ec", e, t);
}
var kr = /* @__PURE__ */ Symbol.for("v-ndc");
function K(e, t, n, r) {
	let i, a = n && n[r], o = d(e);
	if (o || g(e)) {
		let n = o && /* @__PURE__ */ Dt(e), r = !1, s = !1;
		n && (r = !/* @__PURE__ */ kt(e), s = /* @__PURE__ */ Ot(e), e = Ge(e)), i = Array(e.length);
		for (let n = 0, o = e.length; n < o; n++) i[n] = t(r ? s ? Pt(Nt(e[n])) : Nt(e[n]) : e[n], n, void 0, a && a[n]);
	} else if (typeof e == "number") {
		i = Array(e);
		for (let n = 0; n < e; n++) i[n] = t(n + 1, n, void 0, a && a[n]);
	} else if (v(e)) if (e[Symbol.iterator]) i = Array.from(e, (e, n) => t(e, n, void 0, a && a[n]));
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
function Ar(e, t, n = {}, r, i, a) {
	if (gn.ce || gn.parent && dr(gn.parent) && gn.parent.ce) {
		let e = a != null && n.key == null ? s({}, n, { key: a }) : n, i = Object.keys(e).length > 0;
		return t !== "default" && (e.name = t), J(), ea(q, null, [aa("slot", e, r && r())], i ? -2 : 64);
	}
	let o = e[t];
	o && o._c && (o._d = !1);
	let c = Ji.length;
	J();
	let l;
	try {
		let i = o && jr(o(n)), s = n.key || a || i && i.key;
		l = ea(q, { key: (s && !_(s) ? s : `_${t}`) + (!i && r ? "_fb" : "") }, i || (r ? r() : []), i && e._ === 1 ? 64 : -2);
	} catch (e) {
		for (let e = Ji.length; e > c; e--) Xi();
		throw e;
	} finally {
		o && o._c && (o._d = !0);
	}
	return !i && l.scopeId && (l.slotScopeIds = [l.scopeId + "-s"]), l;
}
function jr(e) {
	return e.some((e) => !ta(e) || !(e.type === Ki || e.type === q && !jr(e.children))) ? e : null;
}
var Mr = (e) => e ? Ca(e) ? Na(e) : Mr(e.parent) : null, Nr = /* @__PURE__ */ s(/* @__PURE__ */ Object.create(null), {
	$: (e) => e,
	$el: (e) => e.vnode.el,
	$data: (e) => e.data,
	$props: (e) => e.props,
	$attrs: (e) => e.attrs,
	$slots: (e) => e.slots,
	$refs: (e) => e.refs,
	$parent: (e) => Mr(e.parent),
	$root: (e) => Mr(e.root),
	$host: (e) => e.ce,
	$emit: (e) => e.emit,
	$options: (e) => Hr(e),
	$forceUpdate: (e) => e.f ||= () => {
		ln(e.update);
	},
	$nextTick: (e) => e.n ||= sn.bind(e.proxy),
	$watch: (e) => On.bind(e)
}), Pr = (e, n) => e !== t && !e.__isScriptSetup && u(e, n), Fr = {
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
			else if (Pr(i, n)) return s[n] = 1, i[n];
			else if (a !== t && u(a, n)) return s[n] = 2, a[n];
			else if (u(o, n)) return s[n] = 3, o[n];
			else if (r !== t && u(r, n)) return s[n] = 4, r[n];
			else Lr && (s[n] = 0);
		}
		let d = Nr[n], f, p;
		if (d) return n === "$attrs" && He(e.attrs, "get", ""), d(e);
		if ((f = c.__cssModules) && (f = f[n])) return f;
		if (r !== t && u(r, n)) return s[n] = 4, r[n];
		if (p = l.config.globalProperties, u(p, n)) return p[n];
	},
	set({ _: e }, n, r) {
		let { data: i, setupState: a, ctx: o } = e;
		return Pr(a, n) ? (a[n] = r, !0) : i !== t && u(i, n) ? (i[n] = r, !0) : u(e.props, n) || n[0] === "$" && n.slice(1) in e ? !1 : (o[n] = r, !0);
	},
	has({ _: { data: e, setupState: n, accessCache: r, ctx: i, appContext: a, props: o, type: s } }, c) {
		let l;
		return !!(r[c] || e !== t && c[0] !== "$" && u(e, c) || Pr(n, c) || u(o, c) || u(i, c) || u(Nr, c) || u(a.config.globalProperties, c) || (l = s.__cssModules) && l[c]);
	},
	defineProperty(e, t, n) {
		return n.get == null ? u(n, "value") && this.set(e, t, n.value, null) : e._.accessCache[t] = 0, Reflect.defineProperty(e, t, n);
	}
};
function Ir(e) {
	return d(e) ? e.reduce((e, t) => (e[t] = null, e), {}) : e;
}
var Lr = !0;
function Rr(e) {
	let t = Hr(e), n = e.proxy, i = e.ctx;
	Lr = !1, t.beforeCreate && Br(t.beforeCreate, e, "bc");
	let { data: a, computed: o, methods: s, watch: c, provide: l, inject: u, created: f, beforeMount: p, mounted: m, beforeUpdate: g, updated: _, activated: y, deactivated: b, beforeDestroy: x, beforeUnmount: S, destroyed: C, unmounted: w, render: T, renderTracked: E, renderTriggered: D, errorCaptured: O, serverPrefetch: k, expose: A, inheritAttrs: ee, components: j, directives: M, filters: te } = t;
	if (u && zr(u, i, null), s) for (let e in s) {
		let t = s[e];
		h(t) && (i[e] = t.bind(n));
	}
	if (a) {
		let t = a.call(n, n);
		v(t) && (e.data = /* @__PURE__ */ Ct(t));
	}
	if (Lr = !0, o) for (let e in o) {
		let t = o[e], a = $({
			get: h(t) ? t.bind(n, n) : h(t.get) ? t.get.bind(n, n) : r,
			set: !h(t) && h(t.set) ? t.set.bind(n) : r
		});
		Object.defineProperty(i, e, {
			enumerable: !0,
			configurable: !0,
			get: () => a.value,
			set: (e) => a.value = e
		});
	}
	if (c) for (let e in c) Vr(c[e], i, n, e);
	if (l) {
		let e = h(l) ? l.call(n) : l;
		Reflect.ownKeys(e).forEach((t) => {
			Sn(t, e[t]);
		});
	}
	f && Br(f, e, "c");
	function N(e, t) {
		d(t) ? t.forEach((t) => e(t.bind(n))) : t && e(t.bind(n));
	}
	if (N(yr, p), N(br, m), N(xr, g), N(Sr, _), N(pr, y), N(mr, b), N(Or, O), N(Dr, E), N(Er, D), N(Cr, S), N(wr, w), N(Tr, k), d(A)) if (A.length) {
		let t = e.exposed ||= {};
		A.forEach((e) => {
			Object.defineProperty(t, e, {
				get: () => n[e],
				set: (t) => n[e] = t,
				enumerable: !0
			});
		});
	} else e.exposed ||= {};
	T && e.render === r && (e.render = T), ee != null && (e.inheritAttrs = ee), j && (e.components = j), M && (e.directives = M), k && or(e);
}
function zr(e, t, n = r) {
	d(e) && (e = qr(e));
	for (let n in e) {
		let r = e[n], i;
		i = v(r) ? "default" in r ? Cn(r.from || n, r.default, !0) : Cn(r.from || n) : Cn(r), /* @__PURE__ */ Ft(i) ? Object.defineProperty(t, n, {
			enumerable: !0,
			configurable: !0,
			get: () => i.value,
			set: (e) => i.value = e
		}) : t[n] = i;
	}
}
function Br(e, t, n) {
	Xt(d(e) ? e.map((e) => e.bind(t.proxy)) : e.bind(t.proxy), t, n);
}
function Vr(e, t, n, r) {
	let i = r.includes(".") ? kn(n, r) : () => n[r];
	if (g(e)) {
		let n = t[e];
		h(n) && En(i, n);
	} else if (h(e)) En(i, e.bind(n));
	else if (v(e)) if (d(e)) e.forEach((e) => Vr(e, t, n, r));
	else {
		let r = h(e.handler) ? e.handler.bind(n) : t[e.handler];
		h(r) && En(i, r, e);
	}
}
function Hr(e) {
	let t = e.type, { mixins: n, extends: r } = t, { mixins: i, optionsCache: a, config: { optionMergeStrategies: o } } = e.appContext, s = a.get(t), c;
	return s ? c = s : !i.length && !n && !r ? c = t : (c = {}, i.length && i.forEach((e) => Ur(c, e, o, !0)), Ur(c, t, o)), v(t) && a.set(t, c), c;
}
function Ur(e, t, n, r = !1) {
	let { mixins: i, extends: a } = t;
	a && Ur(e, a, n, !0), i && i.forEach((t) => Ur(e, t, n, !0));
	for (let i in t) if (!(r && i === "expose")) {
		let r = Wr[i] || n && n[i];
		e[i] = r ? r(e[i], t[i]) : t[i];
	}
	return e;
}
var Wr = {
	data: Gr,
	props: Xr,
	emits: Xr,
	methods: Yr,
	computed: Yr,
	beforeCreate: Jr,
	created: Jr,
	beforeMount: Jr,
	mounted: Jr,
	beforeUpdate: Jr,
	updated: Jr,
	beforeDestroy: Jr,
	beforeUnmount: Jr,
	destroyed: Jr,
	unmounted: Jr,
	activated: Jr,
	deactivated: Jr,
	errorCaptured: Jr,
	serverPrefetch: Jr,
	components: Yr,
	directives: Yr,
	watch: Zr,
	provide: Gr,
	inject: Kr
};
function Gr(e, t) {
	return t ? e ? function() {
		return s(h(e) ? e.call(this, this) : e, h(t) ? t.call(this, this) : t);
	} : t : e;
}
function Kr(e, t) {
	return Yr(qr(e), qr(t));
}
function qr(e) {
	if (d(e)) {
		let t = {};
		for (let n = 0; n < e.length; n++) t[e[n]] = e[n];
		return t;
	}
	return e;
}
function Jr(e, t) {
	return e ? [...new Set([].concat(e, t))] : t;
}
function Yr(e, t) {
	return e ? s(/* @__PURE__ */ Object.create(null), e, t) : t;
}
function Xr(e, t) {
	return e ? d(e) && d(t) ? [.../* @__PURE__ */ new Set([...e, ...t])] : s(/* @__PURE__ */ Object.create(null), Ir(e), Ir(t ?? {})) : t;
}
function Zr(e, t) {
	if (!e) return t;
	if (!t) return e;
	let n = s(/* @__PURE__ */ Object.create(null), e);
	for (let r in t) n[r] = Jr(e[r], t[r]);
	return n;
}
function Qr() {
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
var $r = 0;
function ei(e, t) {
	return function(n, r = null) {
		h(n) || (n = s({}, n)), r != null && !v(r) && (r = null);
		let i = Qr(), a = /* @__PURE__ */ new WeakSet(), o = [], c = !1, l = i.app = {
			_uid: $r++,
			_component: n,
			_props: r,
			_container: null,
			_context: i,
			_instance: null,
			version: Ia,
			get config() {
				return i.config;
			},
			set config(e) {},
			use(e, ...t) {
				return a.has(e) || (e && h(e.install) ? (a.add(e), e.install(l, ...t)) : h(e) && (a.add(e), e(l, ...t))), l;
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
					let u = l._ceVNode || aa(n, r);
					return u.appContext = i, s === !0 ? s = "svg" : s === !1 && (s = void 0), o && t ? t(u, a) : e(u, a, s), c = !0, l._container = a, a.__vue_app__ = l, Na(u.component);
				}
			},
			onUnmount(e) {
				o.push(e);
			},
			unmount() {
				c && (Xt(o, l._instance, 16), e(null, l._container), delete l._container.__vue_app__);
			},
			provide(e, t) {
				return i.provides[e] = t, l;
			},
			runWithContext(e) {
				let t = ti;
				ti = l;
				try {
					return e();
				} finally {
					ti = t;
				}
			}
		};
		return l;
	};
}
var ti = null, ni = (e, t) => t === "modelValue" || t === "model-value" ? e.modelModifiers : e[`${t}Modifiers`] || e[`${O(t)}Modifiers`] || e[`${A(t)}Modifiers`];
function ri(e, n, ...r) {
	if (e.isUnmounted) return;
	let i = e.vnode.props || t, a = r, o = n.startsWith("update:"), s = o && ni(i, n.slice(7));
	s && (s.trim && (a = r.map((e) => g(e) ? e.trim() : e)), s.number && (a = r.map(ne)));
	let c, l = i[c = j(n)] || i[c = j(O(n))];
	!l && o && (l = i[c = j(A(n))]), l && Xt(l, e, 6, a);
	let u = i[c + "Once"];
	if (u) {
		if (!e.emitted) e.emitted = {};
		else if (e.emitted[c]) return;
		e.emitted[c] = !0, Xt(u, e, 6, a);
	}
}
var ii = /* @__PURE__ */ new WeakMap();
function ai(e, t, n = !1) {
	let r = n ? ii : t.emitsCache, i = r.get(e);
	if (i !== void 0) return i;
	let a = e.emits, o = {}, c = !1;
	if (!h(e)) {
		let r = (e) => {
			let n = ai(e, t, !0);
			n && (c = !0, s(o, n));
		};
		!n && t.mixins.length && t.mixins.forEach(r), e.extends && r(e.extends), e.mixins && e.mixins.forEach(r);
	}
	return !a && !c ? (v(e) && r.set(e, null), null) : (d(a) ? a.forEach((e) => o[e] = null) : s(o, a), v(e) && r.set(e, o), o);
}
function oi(e, t) {
	return !e || !a(t) ? !1 : (t = t.slice(2), t = t === "Once" ? t : t.replace(/Once$/, ""), u(e, t[0].toLowerCase() + t.slice(1)) || u(e, A(t)) || u(e, t));
}
function si(e) {
	let { type: t, vnode: n, proxy: r, withProxy: i, propsOptions: [a], slots: s, attrs: c, emit: l, render: u, renderCache: d, props: f, data: p, setupState: m, ctx: h, inheritAttrs: g } = e, _ = vn(e), v, y;
	try {
		if (n.shapeFlag & 4) {
			let e = i || r, t = e;
			v = la(u.call(t, e, d, f, m, p, h)), y = c;
		} else {
			let e = t;
			v = la(e.length > 1 ? e(f, {
				attrs: c,
				slots: s,
				emit: l
			}) : e(f, null)), y = t.props ? c : ci(c);
		}
	} catch (t) {
		Ji.length = 0, Zt(t, e, 1), v = aa(Ki);
	}
	let b = v;
	if (y && g !== !1) {
		let e = Object.keys(y), { shapeFlag: t } = b;
		e.length && t & 7 && (a && e.some(o) && (y = li(y, a)), b = ca(b, y, !1, !0));
	}
	return n.dirs && (b = ca(b, null, !1, !0), b.dirs = b.dirs ? b.dirs.concat(n.dirs) : n.dirs), n.transition && rr(b, n.transition), v = b, vn(_), v;
}
var ci = (e) => {
	let t;
	for (let n in e) (n === "class" || n === "style" || a(n)) && ((t ||= {})[n] = e[n]);
	return t;
}, li = (e, t) => {
	let n = {};
	for (let r in e) (!o(r) || !(r.slice(9) in t)) && (n[r] = e[r]);
	return n;
};
function ui(e, t, n) {
	let { props: r, children: i, component: a } = e, { props: o, children: s, patchFlag: c } = t, l = a.emitsOptions;
	if (t.dirs || t.transition) return !0;
	if (n && c >= 0) {
		if (c & 1024) return !0;
		if (c & 16) return r ? di(r, o, l) : !!o;
		if (c & 8) {
			let e = t.dynamicProps;
			for (let t = 0; t < e.length; t++) {
				let n = e[t];
				if (fi(o, r, n) && !oi(l, n)) return !0;
			}
		}
	} else return (i || s) && (!s || !s.$stable) ? !0 : r === o ? !1 : r ? !o || di(r, o, l) : !!o;
	return !1;
}
function di(e, t, n) {
	let r = Object.keys(t);
	if (r.length !== Object.keys(e).length) return !0;
	for (let i = 0; i < r.length; i++) {
		let a = r[i];
		if (fi(t, e, a) && !oi(n, a)) return !0;
	}
	return !1;
}
function fi(e, t, n) {
	let r = e[n], i = t[n];
	return n === "style" && v(r) && v(i) ? !H(r, i) : r !== i;
}
function pi({ vnode: e, parent: t, suspense: n }, r) {
	for (; t;) {
		let n = t.subTree;
		if (n.suspense && n.suspense.activeBranch === e && (n.suspense.vnode.el = n.el = r, e = n), n === e) (e = t.vnode).el = r, t = t.parent;
		else break;
	}
	n && n.activeBranch === e && (n.vnode.el = r);
}
var mi = {}, hi = () => Object.create(mi), gi = (e) => Object.getPrototypeOf(e) === mi;
function _i(e, t, n, r = !1) {
	let i = {}, a = hi();
	e.propsDefaults = /* @__PURE__ */ Object.create(null), yi(e, t, i, a);
	for (let t in e.propsOptions[0]) t in i || (i[t] = void 0);
	e.props = n ? r ? i : /* @__PURE__ */ wt(i) : e.type.props ? i : a, e.attrs = a;
}
function vi(e, t, n, r) {
	let { props: i, attrs: a, vnode: { patchFlag: o } } = e, s = /* @__PURE__ */ jt(i), [c] = e.propsOptions, l = !1;
	if ((r || o > 0) && !(o & 16)) {
		if (o & 8) {
			let n = e.vnode.dynamicProps;
			for (let r = 0; r < n.length; r++) {
				let o = n[r];
				if (oi(e.emitsOptions, o)) continue;
				let d = t[o];
				if (c) if (u(a, o)) d !== a[o] && (a[o] = d, l = !0);
				else {
					let t = O(o);
					i[t] = bi(c, s, t, d, e, !1);
				}
				else d !== a[o] && (a[o] = d, l = !0);
			}
		}
	} else {
		yi(e, t, i, a) && (l = !0);
		let r;
		for (let a in s) (!t || !u(t, a) && ((r = A(a)) === a || !u(t, r))) && (c ? n && (n[a] !== void 0 || n[r] !== void 0) && (i[a] = bi(c, s, a, void 0, e, !0)) : delete i[a]);
		if (a !== s) for (let e in a) (!t || !u(t, e)) && (delete a[e], l = !0);
	}
	l && Ue(e.attrs, "set", "");
}
function yi(e, n, r, i) {
	let [a, o] = e.propsOptions, s = !1, c;
	if (n) for (let t in n) {
		if (T(t)) continue;
		let l = n[t], d;
		a && u(a, d = O(t)) ? !o || !o.includes(d) ? r[d] = l : (c ||= {})[d] = l : oi(e.emitsOptions, t) || (!(t in i) || l !== i[t]) && (i[t] = l, s = !0);
	}
	if (o) {
		let n = /* @__PURE__ */ jt(r), i = c || t;
		for (let t = 0; t < o.length; t++) {
			let s = o[t];
			r[s] = bi(a, n, s, i[s], e, !u(i, s));
		}
	}
	return s;
}
function bi(e, t, n, r, i, a) {
	let o = e[n];
	if (o != null) {
		let e = u(o, "default");
		if (e && r === void 0) {
			let e = o.default;
			if (o.type !== Function && !o.skipFactory && h(e)) {
				let { propsDefaults: a } = i;
				if (n in a) r = a[n];
				else {
					let o = xa(i);
					r = a[n] = e.call(null, t), o();
				}
			} else r = e;
			i.ce && i.ce._setProp(n, r);
		}
		o[0] && (a && !e ? r = !1 : o[1] && (r === "" || r === A(n)) && (r = !0));
	}
	return r;
}
var xi = /* @__PURE__ */ new WeakMap();
function Si(e, r, i = !1) {
	let a = i ? xi : r.propsCache, o = a.get(e);
	if (o) return o;
	let c = e.props, l = {}, f = [], p = !1;
	if (!h(e)) {
		let t = (e) => {
			p = !0;
			let [t, n] = Si(e, r, !0);
			s(l, t), n && f.push(...n);
		};
		!i && r.mixins.length && r.mixins.forEach(t), e.extends && t(e.extends), e.mixins && e.mixins.forEach(t);
	}
	if (!c && !p) return v(e) && a.set(e, n), n;
	if (d(c)) for (let e = 0; e < c.length; e++) {
		let n = O(c[e]);
		Ci(n) && (l[n] = t);
	}
	else if (c) for (let e in c) {
		let t = O(e);
		if (Ci(t)) {
			let n = c[e], r = l[t] = d(n) || h(n) ? { type: n } : s({}, n), i = r.type, a = !1, o = !0;
			if (d(i)) for (let e = 0; e < i.length; ++e) {
				let t = i[e], n = h(t) && t.name;
				if (n === "Boolean") {
					a = !0;
					break;
				}
				n === "String" && (o = !1);
			}
			else a = h(i) && i.name === "Boolean";
			r[0] = a, r[1] = o, (a || u(r, "default")) && f.push(t);
		}
	}
	let m = [l, f];
	return v(e) && a.set(e, m), m;
}
function Ci(e) {
	return e[0] !== "$" && !T(e);
}
var wi = (e) => e === "_" || e === "_ctx" || e === "$stable", Ti = (e) => d(e) ? e.map(la) : [la(e)], Ei = (e, t, n) => {
	if (t._n) return t;
	let r = yn((...e) => Ti(t(...e)), n);
	return r._c = !1, r;
}, Di = (e, t, n) => {
	let r = e._ctx;
	for (let n in e) {
		if (wi(n)) continue;
		let i = e[n];
		if (h(i)) t[n] = Ei(n, i, r);
		else if (i != null) {
			let e = Ti(i);
			t[n] = () => e;
		}
	}
}, Oi = (e, t) => {
	let n = Ti(t);
	e.slots.default = () => n;
}, ki = (e, t, n) => {
	for (let r in t) (n || !wi(r)) && (e[r] = t[r]);
}, Ai = (e, t, n) => {
	let r = e.slots = hi();
	if (e.vnode.shapeFlag & 32) {
		let e = t._;
		e ? (ki(r, t, n), n && N(r, "_", e, !0)) : Di(t, r);
	} else t && Oi(e, t);
}, ji = (e, n, r) => {
	let { vnode: i, slots: a } = e, o = !0, s = t;
	if (i.shapeFlag & 32) {
		let e = n._;
		e ? r && e === 1 ? o = !1 : ki(a, n, r) : (o = !n.$stable, Di(n, a)), s = n;
	} else n && (Oi(e, n), s = { default: 1 });
	if (o) for (let e in a) !wi(e) && s[e] == null && delete a[e];
}, Mi = Wi;
function Ni(e) {
	return Pi(e);
}
function Pi(e, i) {
	let a = F();
	a.__VUE__ = !0;
	let { insert: o, remove: s, patchProp: c, createElement: l, createText: u, createComment: d, setText: f, setElementText: p, parentNode: m, nextSibling: h, setScopeId: g = r, insertStaticContent: _ } = e, v = (e, t, n, r = null, i = null, a = null, o = void 0, s = null, c = !!t.dynamicChildren) => {
		if (e === t) return;
		e && !na(e, t) && (r = se(e), R(e, i, a, !0), e = null), t.patchFlag === -2 && (c = !1, t.dynamicChildren = null);
		let { type: l, ref: u, shapeFlag: d } = t;
		switch (l) {
			case Gi:
				y(e, t, n, r);
				break;
			case Ki:
				b(e, t, n, r);
				break;
			case qi:
				e ?? x(t, n, r, o);
				break;
			case q:
				j(e, t, n, r, i, a, o, s, c);
				break;
			default: d & 1 ? w(e, t, n, r, i, a, o, s, c) : d & 6 ? M(e, t, n, r, i, a, o, s, c) : (d & 64 || d & 128) && l.process(e, t, n, r, i, a, o, s, c, le);
		}
		u != null && i ? lr(u, e && e.ref, a, t || e, !t) : u == null && e && e.ref != null && lr(e.ref, null, a, e, !0);
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
		if (t.type === "svg" ? o = "svg" : t.type === "math" && (o = "mathml"), e == null) E(t, n, r, i, a, o, s, c);
		else {
			let n = e.el && e.el._isVueCE ? e.el : null;
			try {
				n && n._beginPatch(), k(e, t, i, a, o, s, c);
			} finally {
				n && n._endPatch();
			}
		}
	}, E = (e, t, n, r, i, a, s, u) => {
		let d, f, { props: m, shapeFlag: h, transition: g, dirs: _ } = e;
		if (d = e.el = l(e.type, a, m && m.is, m), h & 8 ? p(d, e.children) : h & 16 && O(e.children, d, null, r, i, Fi(e, a), s, u), _ && xn(e, null, r, "created"), D(d, e, e.scopeId, s, r), m) {
			for (let e in m) e !== "value" && !T(e) && c(d, e, null, m[e], a, r);
			"value" in m && c(d, "value", null, m.value, a), (f = m.onVnodeBeforeMount) && pa(f, r, e);
		}
		_ && xn(e, null, r, "beforeMount");
		let v = Li(i, g);
		v && g.beforeEnter(d), o(d, t, n), ((f = m && m.onVnodeMounted) || v || _) && Mi(() => {
			try {
				f && pa(f, r, e), v && g.enter(d), _ && xn(e, null, r, "mounted");
			} finally {}
		}, i);
	}, D = (e, t, n, r, i) => {
		if (n && g(e, n), r) for (let t = 0; t < r.length; t++) g(e, r[t]);
		if (i) {
			let n = i.subTree;
			if (t === n || Ui(n.type) && (n.ssContent === t || n.ssFallback === t)) {
				let t = i.vnode;
				D(e, t, t.scopeId, t.slotScopeIds, i.parent);
			}
		}
	}, O = (e, t, n, r, i, a, o, s, c = 0) => {
		for (let l = c; l < e.length; l++) {
			let c = e[l] = s ? ua(e[l]) : la(e[l]);
			v(null, c, t, n, r, i, a, o, s);
		}
	}, k = (e, n, r, i, a, o, s) => {
		let l = n.el = e.el, { patchFlag: u, dynamicChildren: d, dirs: f } = n;
		u |= e.patchFlag & 16;
		let m = e.props || t, h = n.props || t, g;
		if (r && Ii(r, !1), (g = h.onVnodeBeforeUpdate) && pa(g, r, n, e), f && xn(n, e, r, "beforeUpdate"), r && Ii(r, !0), d && (!e.dynamicChildren || e.dynamicChildren.length !== d.length) && (u = 0, s = !1, d = null), (m.innerHTML && h.innerHTML == null || m.textContent && h.textContent == null) && p(l, ""), d ? A(e.dynamicChildren, d, l, r, i, Fi(n, a), o) : s || I(e, n, l, null, r, i, Fi(n, a), o, !1), u > 0) {
			if (u & 16) ee(l, m, h, r, a);
			else if (u & 2 && m.class !== h.class && c(l, "class", null, h.class, a), u & 4 && c(l, "style", m.style, h.style, a), u & 8) {
				let e = n.dynamicProps;
				for (let t = 0; t < e.length; t++) {
					let n = e[t], i = m[n], o = h[n];
					(o !== i || n === "value") && c(l, n, i, o, a, r);
				}
			}
			u & 1 && e.children !== n.children && p(l, n.children);
		} else !s && d == null && ee(l, m, h, r, a);
		((g = h.onVnodeUpdated) || f) && Mi(() => {
			g && pa(g, r, n, e), f && xn(n, e, r, "updated");
		}, i);
	}, A = (e, t, n, r, i, a, o) => {
		for (let s = 0; s < t.length; s++) {
			let c = e[s], l = t[s], u = c.el && (c.type === q || !na(c, l) || c.shapeFlag & 198) ? m(c.el) : n;
			v(c, l, u, null, r, i, a, o, !0);
		}
	}, ee = (e, n, r, i, a) => {
		if (n !== r) {
			if (n !== t) for (let t in n) !T(t) && !(t in r) && c(e, t, n[t], null, a, i);
			for (let t in r) {
				if (T(t)) continue;
				let o = r[t], s = n[t];
				o !== s && t !== "value" && c(e, t, s, o, a, i);
			}
			"value" in r && c(e, "value", n.value, r.value, a);
		}
	}, j = (e, t, n, r, i, a, s, c, l) => {
		let d = t.el = e ? e.el : u(""), f = t.anchor = e ? e.anchor : u(""), { patchFlag: p, dynamicChildren: m, slotScopeIds: h } = t;
		h && (c = c ? c.concat(h) : h), e == null ? (o(d, n, r), o(f, n, r), O(t.children || [], n, f, i, a, s, c, l)) : p > 0 && p & 64 && m && e.dynamicChildren && e.dynamicChildren.length === m.length ? (A(e.dynamicChildren, m, n, i, a, s, c), (t.key != null || i && t === i.subTree) && Ri(e, t, !0)) : I(e, t, n, f, i, a, s, c, l);
	}, M = (e, t, n, r, i, a, o, s, c) => {
		t.slotScopeIds = s, e == null ? t.shapeFlag & 512 ? i.ctx.activate(t, n, r, o, c) : N(t, n, r, i, a, o, c) : ne(e, t, c);
	}, N = (e, t, n, r, i, a, o) => {
		let s = e.component = ga(e, r, i);
		if (fr(e) && (s.ctx.renderer = le), Ta(s, !1, o), s.asyncDep) {
			if (i && i.registerDep(s, re, o), !e.el) {
				let r = s.subTree = aa(Ki);
				b(null, r, t, n), e.placeholder = r.el;
			}
		} else re(s, e, t, n, i, a, o);
	}, ne = (e, t, n) => {
		let r = t.component = e.component;
		if (ui(e, t, n)) if (r.asyncDep && !r.asyncResolved) {
			P(r, t, n);
			return;
		} else r.next = t, r.update();
		else t.el = e.el, r.vnode = t;
	}, re = (e, t, n, r, i, a, o) => {
		let s = () => {
			if (e.isMounted) {
				let { next: t, bu: n, u: r, parent: s, vnode: c } = e;
				{
					let n = Bi(e);
					if (n) {
						t && (t.el = c.el, P(e, t, o)), n.asyncDep.then(() => {
							Mi(() => {
								e.isUnmounted || l();
							}, i);
						});
						return;
					}
				}
				let u = t, d;
				Ii(e, !1), t ? (t.el = c.el, P(e, t, o)) : t = c, n && te(n), (d = t.props && t.props.onVnodeBeforeUpdate) && pa(d, s, t, c), Ii(e, !0);
				let f = si(e), p = e.subTree;
				e.subTree = f, v(p, f, m(p.el), se(p), e, i, a), t.el = f.el, u === null && pi(e, f.el), r && Mi(r, i), (d = t.props && t.props.onVnodeUpdated) && Mi(() => pa(d, s, t, c), i);
			} else {
				let o, { el: s, props: c } = t, { bm: l, m: u, parent: d, root: f, type: p } = e, m = dr(t);
				if (Ii(e, !1), l && te(l), !m && (o = c && c.onVnodeBeforeMount) && pa(o, d, t), Ii(e, !0), s && ue) {
					let t = () => {
						e.subTree = si(e), ue(s, e.subTree, e, i, null);
					};
					m && p.__asyncHydrate ? p.__asyncHydrate(s, e, t) : t();
				} else {
					f.ce && f.ce._hasShadowRoot() && f.ce._injectChildStyle(p, e.parent ? e.parent.type : void 0);
					let o = e.subTree = si(e);
					v(null, o, n, r, e, i, a), t.el = o.el;
				}
				if (u && Mi(u, i), !m && (o = c && c.onVnodeMounted)) {
					let e = t;
					Mi(() => pa(o, d, e), i);
				}
				(t.shapeFlag & 256 || d && dr(d.vnode) && d.vnode.shapeFlag & 256) && e.a && Mi(e.a, i), e.isMounted = !0, t = n = r = null;
			}
		};
		e.scope.on();
		let c = e.effect = new ge(s);
		e.scope.off();
		let l = e.update = c.run.bind(c), u = e.job = c.runIfDirty.bind(c);
		u.i = e, u.id = e.uid, c.scheduler = () => ln(u), Ii(e, !0), l();
	}, P = (e, t, n) => {
		t.component = e;
		let r = e.vnode.props;
		e.vnode = t, e.next = null, vi(e, t.props, r, n), ji(e, t.children, n), je(), fn(e), Me();
	}, I = (e, t, n, r, i, a, o, s, c = !1) => {
		let l = e && e.children, u = e ? e.shapeFlag : 0, d = t.children, { patchFlag: f, shapeFlag: m } = t;
		if (f > 0) {
			if (f & 128) {
				ie(l, d, n, r, i, a, o, s, c);
				return;
			}
			if (f & 256) {
				L(l, d, n, r, i, a, o, s, c);
				return;
			}
		}
		m & 8 ? (u & 16 && oe(l, i, a), d !== l && p(n, d)) : u & 16 ? m & 16 ? ie(l, d, n, r, i, a, o, s, c) : oe(l, i, a, !0) : (u & 8 && p(n, ""), m & 16 && O(d, n, r, i, a, o, s, c));
	}, L = (e, t, r, i, a, o, s, c, l) => {
		e ||= n, t ||= n;
		let u = e.length, d = t.length, f = Math.min(u, d), p;
		for (p = 0; p < f; p++) {
			let n = t[p] = l ? ua(t[p]) : la(t[p]);
			v(e[p], n, r, null, a, o, s, c, l);
		}
		u > d ? oe(e, a, o, !0, !1, f) : O(t, r, i, a, o, s, c, l, f);
	}, ie = (e, t, r, i, a, o, s, c, l) => {
		let u = 0, d = t.length, f = e.length - 1, p = d - 1;
		for (; u <= f && u <= p;) {
			let n = e[u], i = t[u] = l ? ua(t[u]) : la(t[u]);
			if (na(n, i)) v(n, i, r, null, a, o, s, c, l);
			else break;
			u++;
		}
		for (; u <= f && u <= p;) {
			let n = e[f], i = t[p] = l ? ua(t[p]) : la(t[p]);
			if (na(n, i)) v(n, i, r, null, a, o, s, c, l);
			else break;
			f--, p--;
		}
		if (u > f) {
			if (u <= p) {
				let e = p + 1, n = e < d ? t[e].el : i;
				for (; u <= p;) v(null, t[u] = l ? ua(t[u]) : la(t[u]), r, n, a, o, s, c, l), u++;
			}
		} else if (u > p) for (; u <= f;) R(e[u], a, o, !0), u++;
		else {
			let m = u, h = u, g = /* @__PURE__ */ new Map();
			for (u = h; u <= p; u++) {
				let e = t[u] = l ? ua(t[u]) : la(t[u]);
				e.key != null && g.set(e.key, u);
			}
			let _, y = 0, b = p - h + 1, x = !1, S = 0, C = Array(b);
			for (u = 0; u < b; u++) C[u] = 0;
			for (u = m; u <= f; u++) {
				let n = e[u];
				if (y >= b) {
					R(n, a, o, !0);
					continue;
				}
				let i;
				if (n.key != null) i = g.get(n.key);
				else for (_ = h; _ <= p; _++) if (C[_ - h] === 0 && na(n, t[_])) {
					i = _;
					break;
				}
				i === void 0 ? R(n, a, o, !0) : (C[i - h] = u + 1, i >= S ? S = i : x = !0, v(n, t[i], r, null, a, o, s, c, l), y++);
			}
			let w = x ? zi(C) : n;
			for (_ = w.length - 1, u = b - 1; u >= 0; u--) {
				let e = h + u, n = t[e], f = t[e + 1], p = e + 1 < d ? f.el || Hi(f) : i;
				C[u] === 0 ? v(null, n, r, p, a, o, s, c, l) : x && (_ < 0 || u !== w[_] ? ae(n, r, p, 2) : _--);
			}
		}
	}, ae = (e, t, n, r, i = null) => {
		let { el: a, type: c, transition: l, children: u, shapeFlag: d } = e;
		if (d & 6) {
			ae(e.component.subTree, t, n, r);
			return;
		}
		if (d & 128) {
			e.suspense.move(t, n, r);
			return;
		}
		if (d & 64) {
			c.move(e, t, n, le);
			return;
		}
		if (c === q) {
			o(a, t, n);
			for (let e = 0; e < u.length; e++) ae(u[e], t, n, r);
			o(e.anchor, t, n);
			return;
		}
		if (c === qi) {
			S(e, t, n);
			return;
		}
		if (r !== 2 && d & 1 && l) if (r === 0) l.persisted && !a[Wn] ? o(a, t, n) : (l.beforeEnter(a), o(a, t, n), Mi(() => l.enter(a), i));
		else {
			let { leave: r, delayLeave: i, afterLeave: c } = l, u = () => {
				e.ctx.isUnmounted ? s(a) : o(a, t, n);
			}, d = () => {
				let e = a._isLeaving || !!a[Wn];
				a._isLeaving && a[Wn](!0), l.persisted && !e ? u() : r(a, () => {
					u(), c && c();
				});
			};
			i ? i(a, u, d) : d();
		}
		else o(a, t, n);
	}, R = (e, t, n, r = !1, i = !1) => {
		let { type: a, props: o, ref: s, children: c, dynamicChildren: l, shapeFlag: u, patchFlag: d, dirs: f, cacheIndex: p, memo: m } = e;
		if (d === -2 && (i = !1), s != null && (je(), lr(s, null, n, e, !0), Me()), p != null && (t.renderCache[p] = void 0), u & 256) {
			t.ctx.deactivate(e);
			return;
		}
		let h = u & 1 && f, g = !dr(e), _;
		if (g && (_ = o && o.onVnodeBeforeUnmount) && pa(_, t, e), u & 6) V(e.component, n, r);
		else {
			if (u & 128) {
				e.suspense.unmount(n, r);
				return;
			}
			h && xn(e, null, t, "beforeUnmount"), u & 64 ? e.type.remove(e, t, n, le, r) : l && !l.hasOnce && (a !== q || d > 0 && d & 64) ? oe(l, t, n, !1, !0) : (a === q && d & 384 || !i && u & 16) && oe(c, t, n), r && z(e);
		}
		let v = m != null && p == null;
		(g && (_ = o && o.onVnodeUnmounted) || h || v) && Mi(() => {
			_ && pa(_, t, e), h && xn(e, null, t, "unmounted"), v && (e.el = null);
		}, n);
	}, z = (e) => {
		let { type: t, el: n, anchor: r, transition: i } = e;
		if (t === q) {
			B(n, r);
			return;
		}
		if (t === qi) {
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
	}, V = (e, t, n) => {
		let { bum: r, scope: i, job: a, subTree: o, um: s, m: c, a: l } = e;
		Vi(c), Vi(l), r && te(r), i.stop(), a && (a.flags |= 8, R(o, e, t, n)), s && Mi(s, t), Mi(() => {
			e.isUnmounted = !0;
		}, t);
	}, oe = (e, t, n, r = !1, i = !1, a = 0) => {
		for (let o = a; o < e.length; o++) R(e[o], t, n, r, i);
	}, se = (e) => {
		if (e.shapeFlag & 6) return se(e.component.subTree);
		if (e.shapeFlag & 128) return e.suspense.next();
		let t = h(e.anchor || e.el), n = t && t[jn];
		return n ? h(n) : t;
	}, H = !1, ce = (e, t, n) => {
		let r;
		e == null ? t._vnode && (R(t._vnode, null, null, !0), r = t._vnode.component) : v(t._vnode || null, e, t, null, null, null, n), t._vnode = e, H ||= (H = !0, fn(r), pn(), !1);
	}, le = {
		p: v,
		um: R,
		m: ae,
		r: z,
		mt: N,
		mc: O,
		pc: I,
		pbc: A,
		n: se,
		o: e
	}, U, ue;
	return i && ([U, ue] = i(le)), {
		render: ce,
		hydrate: U,
		createApp: ei(ce, U)
	};
}
function Fi({ type: e, props: t }, n) {
	return n === "svg" && e === "foreignObject" || n === "mathml" && e === "annotation-xml" && t && t.encoding && t.encoding.includes("html") ? void 0 : n;
}
function Ii({ effect: e, job: t }, n) {
	n ? (e.flags |= 32, t.flags |= 4) : (e.flags &= -33, t.flags &= -5);
}
function Li(e, t) {
	return (!e || e && !e.pendingBranch) && t && !t.persisted;
}
function Ri(e, t, n = !1) {
	let r = e.children, i = t.children;
	if (d(r) && d(i)) for (let e = 0; e < r.length; e++) {
		let t = r[e], a = i[e];
		a.shapeFlag & 1 && !a.dynamicChildren && ((a.patchFlag <= 0 || a.patchFlag === 32) && (a = i[e] = ua(i[e]), a.el = t.el), !n && a.patchFlag !== -2 && Ri(t, a)), a.type === Gi && (a.patchFlag === -1 && (a = i[e] = ua(a)), a.el = t.el), a.type === Ki && !a.el && (a.el = t.el);
	}
}
function zi(e) {
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
function Bi(e) {
	let t = e.subTree.component;
	if (t) return t.asyncDep && !t.asyncResolved ? t : Bi(t);
}
function Vi(e) {
	if (e) for (let t = 0; t < e.length; t++) e[t].flags |= 8;
}
function Hi(e) {
	if (e.placeholder) return e.placeholder;
	let t = e.component;
	return t ? Hi(t.subTree) : null;
}
var Ui = (e) => e.__isSuspense;
function Wi(e, t) {
	t && t.pendingBranch ? d(e) ? t.effects.push(...e) : t.effects.push(e) : dn(e);
}
var q = /* @__PURE__ */ Symbol.for("v-fgt"), Gi = /* @__PURE__ */ Symbol.for("v-txt"), Ki = /* @__PURE__ */ Symbol.for("v-cmt"), qi = /* @__PURE__ */ Symbol.for("v-stc"), Ji = [], Yi = null;
function J(e = !1) {
	Ji.push(Yi = e ? null : []);
}
function Xi() {
	Ji.pop(), Yi = Ji[Ji.length - 1] || null;
}
var Zi = 1;
function Qi(e, t = !1) {
	Zi += e, e < 0 && Yi && t && (Yi.hasOnce = !0);
}
function $i(e) {
	return e.dynamicChildren = Zi > 0 ? Yi || n : null, Xi(), Zi > 0 && Yi && Yi.push(e), e;
}
function Y(e, t, n, r, i, a) {
	return $i(X(e, t, n, r, i, a, !0));
}
function ea(e, t, n, r, i) {
	return $i(aa(e, t, n, r, i, !0));
}
function ta(e) {
	return e ? e.__v_isVNode === !0 : !1;
}
function na(e, t) {
	return e.type === t.type && e.key === t.key;
}
var ra = ({ key: e }) => e ?? null, ia = ({ ref: e, ref_key: t, ref_for: n }) => (typeof e == "number" && (e = "" + e), e == null ? null : g(e) || /* @__PURE__ */ Ft(e) || h(e) ? {
	i: gn,
	r: e,
	k: t,
	f: !!n
} : e);
function X(e, t = null, n = null, r = 0, i = null, a = e === q ? 0 : 1, o = !1, s = !1) {
	let c = {
		__v_isVNode: !0,
		__v_skip: !0,
		type: e,
		props: t,
		key: t && ra(t),
		ref: t && ia(t),
		scopeId: _n,
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
		ctx: gn
	};
	return s ? (da(c, n), a & 128 && e.normalize(c)) : n && (c.shapeFlag |= g(n) ? 8 : 16), Zi > 0 && !o && Yi && (c.patchFlag > 0 || a & 6) && c.patchFlag !== 32 && Yi.push(c), c;
}
var aa = oa;
function oa(e, t = null, n = null, r = 0, i = null, a = !1) {
	if ((!e || e === kr) && (e = Ki), ta(e)) {
		let r = ca(e, t, !0);
		return n && da(r, n), Zi > 0 && !a && Yi && (r.shapeFlag & 6 ? Yi[Yi.indexOf(e)] = r : Yi.push(r)), r.patchFlag = -2, r;
	}
	if (Pa(e) && (e = e.__vccOpts), t) {
		t = sa(t);
		let { class: e, style: n } = t;
		e && !g(e) && (t.class = z(e)), v(n) && (/* @__PURE__ */ At(n) && !d(n) && (n = s({}, n)), t.style = I(n));
	}
	let o = g(e) ? 1 : Ui(e) ? 128 : Mn(e) ? 64 : v(e) ? 4 : h(e) ? 2 : 0;
	return X(e, t, n, r, i, o, a, !0);
}
function sa(e) {
	return e ? /* @__PURE__ */ At(e) || gi(e) ? s({}, e) : e : null;
}
function ca(e, t, n = !1, r = !1) {
	let { props: i, ref: a, patchFlag: o, children: s, transition: c } = e, l = t ? fa(i || {}, t) : i, u = {
		__v_isVNode: !0,
		__v_skip: !0,
		type: e.type,
		props: l,
		key: l && ra(l),
		ref: t && t.ref ? n && a ? d(a) ? a.concat(ia(t)) : [a, ia(t)] : ia(t) : a,
		scopeId: e.scopeId,
		slotScopeIds: e.slotScopeIds,
		children: s,
		target: e.target,
		targetStart: e.targetStart,
		targetAnchor: e.targetAnchor,
		staticCount: e.staticCount,
		shapeFlag: e.shapeFlag,
		patchFlag: t && e.type !== q ? o === -1 ? 16 : o | 16 : o,
		dynamicProps: e.dynamicProps,
		dynamicChildren: e.dynamicChildren,
		appContext: e.appContext,
		dirs: e.dirs,
		transition: c,
		component: e.component,
		suspense: e.suspense,
		ssContent: e.ssContent && ca(e.ssContent),
		ssFallback: e.ssFallback && ca(e.ssFallback),
		placeholder: e.placeholder,
		el: e.el,
		anchor: e.anchor,
		ctx: e.ctx,
		ce: e.ce
	};
	return c && r && rr(u, c.clone(u)), u;
}
function Z(e = " ", t = 0) {
	return aa(Gi, null, e, t);
}
function Q(e = "", t = !1) {
	return t ? (J(), ea(Ki, null, e)) : aa(Ki, null, e);
}
function la(e) {
	return e == null || typeof e == "boolean" ? aa(Ki) : d(e) ? aa(q, null, e.slice()) : ta(e) ? ua(e) : aa(Gi, null, String(e));
}
function ua(e) {
	return e.el === null && e.patchFlag !== -1 || e.memo ? e : ca(e);
}
function da(e, t) {
	let n = 0, { shapeFlag: r } = e;
	if (t == null) t = null;
	else if (d(t)) n = 16;
	else if (typeof t == "object") if (r & 65) {
		let n = t.default;
		n && (n._c && (n._d = !1), da(e, n()), n._c && (n._d = !0));
		return;
	} else {
		n = 32;
		let r = t._;
		!r && !gi(t) ? t._ctx = gn : r === 3 && gn && (gn.slots._ === 1 ? t._ = 1 : (t._ = 2, e.patchFlag |= 1024));
	}
	else if (h(t)) {
		if (r & 65) {
			da(e, { default: t });
			return;
		}
		t = {
			default: t,
			_ctx: gn
		}, n = 32;
	} else t = String(t), r & 64 ? (n = 16, t = [Z(t)]) : n = 8;
	e.children = t, e.shapeFlag |= n;
}
function fa(...e) {
	let t = {};
	for (let n = 0; n < e.length; n++) {
		let r = e[n];
		for (let e in r) if (e === "class") t.class !== r.class && (t.class = z([t.class, r.class]));
		else if (e === "style") t.style = I([t.style, r.style]);
		else if (a(e)) {
			let n = t[e], i = r[e];
			i && n !== i && !(d(n) && n.includes(i)) ? t[e] = n ? [].concat(n, i) : i : i == null && n == null && !o(e) && (t[e] = i);
		} else e !== "" && (t[e] = r[e]);
	}
	return t;
}
function pa(e, t, n, r = null) {
	Xt(e, t, 7, [n, r]);
}
var ma = Qr(), ha = 0;
function ga(e, n, r) {
	let i = e.type, a = (n ? n.appContext : e.appContext) || ma, o = {
		uid: ha++,
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
		scope: new pe(!0),
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
		propsOptions: Si(i, a),
		emitsOptions: ai(i, a),
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
	return o.ctx = { _: o }, o.root = n ? n.root : o, o.emit = ri.bind(null, o), e.ce && e.ce(o), o;
}
var _a = null, va = () => _a || gn, ya, ba;
{
	let e = F(), t = (t, n) => {
		let r;
		return (r = e[t]) || (r = e[t] = []), r.push(n), (e) => {
			r.length > 1 ? r.forEach((t) => t(e)) : r[0](e);
		};
	};
	ya = t("__VUE_INSTANCE_SETTERS__", (e) => _a = e), ba = t("__VUE_SSR_SETTERS__", (e) => wa = e);
}
var xa = (e) => {
	let t = _a;
	return ya(e), e.scope.on(), () => {
		e.scope.off(), ya(t);
	};
}, Sa = () => {
	_a && _a.scope.off(), ya(null);
};
function Ca(e) {
	return e.vnode.shapeFlag & 4;
}
var wa = !1;
function Ta(e, t = !1, n = !1) {
	t && ba(t);
	let { props: r, children: i } = e.vnode, a = Ca(e);
	_i(e, r, a, t), Ai(e, i, n || t);
	let o = a ? Ea(e, t) : void 0;
	return t && ba(!1), o;
}
function Ea(e, t) {
	let n = e.type;
	e.accessCache = /* @__PURE__ */ Object.create(null), e.proxy = new Proxy(e.ctx, Fr);
	let { setup: r } = n;
	if (r) {
		je();
		let n = e.setupContext = r.length > 1 ? Ma(e) : null, i = xa(e), a = Yt(r, e, 0, [e.props, n]), o = y(a);
		if (Me(), i(), (o || e.sp) && !dr(e) && or(e), o) {
			if (a.then(Sa, Sa), t) return a.then((n) => {
				Da(e, n, t);
			}).catch((t) => {
				Zt(t, e, 0);
			});
			e.asyncDep = a;
		} else Da(e, a, t);
	} else Aa(e, t);
}
function Da(e, t, n) {
	h(t) ? e.type.__ssrInlineRender ? e.ssrRender = t : e.render = t : v(t) && (e.setupState = Bt(t)), Aa(e, n);
}
var Oa, ka;
function Aa(e, t, n) {
	let i = e.type;
	if (!e.render) {
		if (!t && Oa && !i.render) {
			let t = i.template || Hr(e).template;
			if (t) {
				let { isCustomElement: n, compilerOptions: r } = e.appContext.config, { delimiters: a, compilerOptions: o } = i;
				i.render = Oa(t, s(s({
					isCustomElement: n,
					delimiters: a
				}, r), o));
			}
		}
		e.render = i.render || r, ka && ka(e);
	}
	{
		let t = xa(e);
		je();
		try {
			Rr(e);
		} finally {
			Me(), t();
		}
	}
}
var ja = { get(e, t) {
	return He(e, "get", ""), e[t];
} };
function Ma(e) {
	return {
		attrs: new Proxy(e.attrs, ja),
		slots: e.slots,
		emit: e.emit,
		expose: (t) => {
			e.exposed = t || {};
		}
	};
}
function Na(e) {
	return e.exposed ? e.exposeProxy ||= new Proxy(Bt(Mt(e.exposed)), {
		get(t, n) {
			if (n in t) return t[n];
			if (n in Nr) return Nr[n](e);
		},
		has(e, t) {
			return t in e || t in Nr;
		}
	}) : e.proxy;
}
function Pa(e) {
	return h(e) && "__vccOpts" in e;
}
var $ = (e, t) => /* @__PURE__ */ Ht(e, t, wa);
function Fa(e, t, n) {
	try {
		Qi(-1);
		let r = arguments.length;
		return r === 2 ? v(t) && !d(t) ? ta(t) ? aa(e, null, [t]) : aa(e, t) : aa(e, null, t) : (r > 3 ? n = Array.prototype.slice.call(arguments, 2) : r === 3 && ta(n) && (n = [n]), aa(e, t, n));
	} finally {
		Qi(1);
	}
}
var Ia = "3.5.40", La = void 0, Ra = typeof window < "u" && window.trustedTypes;
if (Ra) try {
	La = /* @__PURE__ */ Ra.createPolicy("vue", { createHTML: (e) => e });
} catch {}
var za = La ? (e) => La.createHTML(e) : (e) => e, Ba = "http://www.w3.org/2000/svg", Va = "http://www.w3.org/1998/Math/MathML", Ha = typeof document < "u" ? document : null, Ua = Ha && /* @__PURE__ */ Ha.createElement("template"), Wa = {
	insert: (e, t, n) => {
		t.insertBefore(e, n || null);
	},
	remove: (e) => {
		let t = e.parentNode;
		t && t.removeChild(e);
	},
	createElement: (e, t, n, r) => {
		let i = t === "svg" ? Ha.createElementNS(Ba, e) : t === "mathml" ? Ha.createElementNS(Va, e) : n ? Ha.createElement(e, { is: n }) : Ha.createElement(e);
		return e === "select" && r && r.multiple != null && i.setAttribute("multiple", r.multiple), i;
	},
	createText: (e) => Ha.createTextNode(e),
	createComment: (e) => Ha.createComment(e),
	setText: (e, t) => {
		e.nodeValue = t;
	},
	setElementText: (e, t) => {
		e.textContent = t;
	},
	parentNode: (e) => e.parentNode,
	nextSibling: (e) => e.nextSibling,
	querySelector: (e) => Ha.querySelector(e),
	setScopeId(e, t) {
		e.setAttribute(t, "");
	},
	insertStaticContent(e, t, n, r, i, a) {
		let o = n ? n.previousSibling : t.lastChild;
		if (i && (i === a || i.nextSibling)) for (; t.insertBefore(i.cloneNode(!0), n), !(i === a || !(i = i.nextSibling)););
		else {
			Ua.innerHTML = za(r === "svg" ? `<svg>${e}</svg>` : r === "mathml" ? `<math>${e}</math>` : e);
			let i = Ua.content;
			if (r === "svg" || r === "mathml") {
				let e = i.firstChild;
				for (; e.firstChild;) i.appendChild(e.firstChild);
				i.removeChild(e);
			}
			t.insertBefore(i, n);
		}
		return [o ? o.nextSibling : t.firstChild, n ? n.previousSibling : t.lastChild];
	}
}, Ga = "transition", Ka = "animation", qa = /* @__PURE__ */ Symbol("_vtc"), Ja = {
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
}, Ya = /* @__PURE__ */ s({}, Jn, Ja), Xa = /* @__PURE__ */ ((e) => (e.displayName = "Transition", e.props = Ya, e))((e, { slots: t }) => Fa(Qn, $a(e), t)), Za = (e, t = []) => {
	d(e) ? e.forEach((e) => e(...t)) : e && e(...t);
}, Qa = (e) => e ? d(e) ? e.some((e) => e.length > 1) : e.length > 1 : !1;
function $a(e) {
	let t = {};
	for (let n in e) n in Ja || (t[n] = e[n]);
	if (e.css === !1) return t;
	let { name: n = "v", type: r, duration: i, enterFromClass: a = `${n}-enter-from`, enterActiveClass: o = `${n}-enter-active`, enterToClass: c = `${n}-enter-to`, appearFromClass: l = a, appearActiveClass: u = o, appearToClass: d = c, leaveFromClass: f = `${n}-leave-from`, leaveActiveClass: p = `${n}-leave-active`, leaveToClass: m = `${n}-leave-to` } = e, h = eo(i), g = h && h[0], _ = h && h[1], { onBeforeEnter: v, onEnter: y, onEnterCancelled: b, onLeave: x, onLeaveCancelled: S, onBeforeAppear: C = v, onAppear: w = y, onAppearCancelled: T = b } = t, E = (e, t, n, r) => {
		e._enterCancelled = r, ro(e, t ? d : c), ro(e, t ? u : o), n && n();
	}, D = (e, t) => {
		e._isLeaving = !1, ro(e, f), ro(e, m), ro(e, p), t && t();
	}, O = (e) => (t, n) => {
		let i = e ? w : y, o = () => E(t, e, n);
		Za(i, [t, o]), io(() => {
			ro(t, e ? l : a), no(t, e ? d : c), Qa(i) || oo(t, r, g, o);
		});
	};
	return s(t, {
		onBeforeEnter(e) {
			Za(v, [e]), no(e, a), no(e, o);
		},
		onBeforeAppear(e) {
			Za(C, [e]), no(e, l), no(e, u);
		},
		onEnter: O(!1),
		onAppear: O(!0),
		onLeave(e, t) {
			e._isLeaving = !0;
			let n = () => D(e, t);
			no(e, f), e._enterCancelled ? (no(e, p), uo(e)) : (uo(e), no(e, p)), io(() => {
				e._isLeaving && (ro(e, f), no(e, m), Qa(x) || oo(e, r, _, n));
			}), Za(x, [e, n]);
		},
		onEnterCancelled(e) {
			E(e, !1, void 0, !0), Za(b, [e]);
		},
		onAppearCancelled(e) {
			E(e, !0, void 0, !0), Za(T, [e]);
		},
		onLeaveCancelled(e) {
			D(e), Za(S, [e]);
		}
	});
}
function eo(e) {
	if (e == null) return null;
	if (v(e)) return [to(e.enter), to(e.leave)];
	{
		let t = to(e);
		return [t, t];
	}
}
function to(e) {
	return re(e);
}
function no(e, t) {
	t.split(/\s+/).forEach((t) => t && e.classList.add(t)), (e[qa] || (e[qa] = /* @__PURE__ */ new Set())).add(t);
}
function ro(e, t) {
	t.split(/\s+/).forEach((t) => t && e.classList.remove(t));
	let n = e[qa];
	n && (n.delete(t), n.size || (e[qa] = void 0));
}
function io(e) {
	requestAnimationFrame(() => {
		requestAnimationFrame(e);
	});
}
var ao = 0;
function oo(e, t, n, r) {
	let i = e._endId = ++ao, a = () => {
		i === e._endId && r();
	};
	if (n != null) return setTimeout(a, n);
	let { type: o, timeout: s, propCount: c } = so(e, t);
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
function so(e, t) {
	let n = window.getComputedStyle(e), r = (e) => (n[e] || "").split(", "), i = r(`${Ga}Delay`), a = r(`${Ga}Duration`), o = co(i, a), s = r(`${Ka}Delay`), c = r(`${Ka}Duration`), l = co(s, c), u = null, d = 0, f = 0;
	t === Ga ? o > 0 && (u = Ga, d = o, f = a.length) : t === Ka ? l > 0 && (u = Ka, d = l, f = c.length) : (d = Math.max(o, l), u = d > 0 ? o > l ? Ga : Ka : null, f = u ? u === Ga ? a.length : c.length : 0);
	let p = u === Ga && /\b(?:transform|all)(?:,|$)/.test(r(`${Ga}Property`).toString());
	return {
		type: u,
		timeout: d,
		propCount: f,
		hasTransform: p
	};
}
function co(e, t) {
	for (; e.length < t.length;) e = e.concat(e);
	return Math.max(...t.map((t, n) => lo(t) + lo(e[n])));
}
function lo(e) {
	return e === "auto" ? 0 : Number(e.slice(0, -1).replace(",", ".")) * 1e3;
}
function uo(e) {
	return (e ? e.ownerDocument : document).body.offsetHeight;
}
function fo(e, t, n) {
	let r = e[qa];
	r && (t = (t ? [t, ...r] : [...r]).join(" ")), t == null ? e.removeAttribute("class") : n ? e.setAttribute("class", t) : e.className = t;
}
var po = /* @__PURE__ */ Symbol("_vod"), mo = /* @__PURE__ */ Symbol("_vsh"), ho = /* @__PURE__ */ Symbol(""), go = /(?:^|;)\s*display\s*:/;
function _o(e, t, n) {
	let r = e.style, i = g(n), a = !1;
	if (n && !i) {
		if (t) if (g(t)) for (let e of t.split(";")) {
			let t = e.slice(0, e.indexOf(":")).trim();
			n[t] ?? yo(r, t, "");
		}
		else for (let e in t) n[e] ?? yo(r, e, "");
		for (let i in n) {
			i === "display" && (a = !0);
			let o = n[i];
			o == null ? yo(r, i, "") : Co(e, i, !g(t) && t ? t[i] : void 0, o) || yo(r, i, o);
		}
	} else if (i) {
		if (t !== n) {
			let e = r[ho];
			e && (n += ";" + e), r.cssText = n, a = go.test(n);
		}
	} else t && e.removeAttribute("style");
	po in e && (e[po] = a ? r.display : "", e[mo] && (r.display = "none"));
}
var vo = /\s*!important$/;
function yo(e, t, n) {
	if (d(n)) n.forEach((n) => yo(e, t, n));
	else if (n ??= "", t.startsWith("--")) e.setProperty(t, n);
	else {
		let r = So(e, t);
		vo.test(n) ? e.setProperty(A(r), n.replace(vo, ""), "important") : e[r] = n;
	}
}
var bo = [
	"Webkit",
	"Moz",
	"ms"
], xo = {};
function So(e, t) {
	let n = xo[t];
	if (n) return n;
	let r = O(t);
	if (r !== "filter" && r in e) return xo[t] = r;
	r = ee(r);
	for (let n = 0; n < bo.length; n++) {
		let i = bo[n] + r;
		if (i in e) return xo[t] = i;
	}
	return t;
}
function Co(e, t, n, r) {
	return e.tagName === "TEXTAREA" && (t === "width" || t === "height") && g(r) && n === r;
}
var wo = "http://www.w3.org/1999/xlink";
function To(e, t, n, r, i, a = V(t)) {
	r && t.startsWith("xlink:") ? n == null ? e.removeAttributeNS(wo, t.slice(6, t.length)) : e.setAttributeNS(wo, t, n) : n == null || a && !oe(n) ? e.removeAttribute(t) : e.setAttribute(t, a ? "" : _(n) ? String(n) : n);
}
function Eo(e, t, n, r, i) {
	if (t === "innerHTML" || t === "textContent") {
		n != null && (e[t] = t === "innerHTML" ? za(n) : n);
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
		r === "boolean" ? n = oe(n) : n == null && r === "string" ? (n = "", o = !0) : r === "number" && (n = 0, o = !0);
	}
	try {
		e[t] = n;
	} catch {}
	o && e.removeAttribute(i || t);
}
function Do(e, t, n, r) {
	e.addEventListener(t, n, r);
}
function Oo(e, t, n, r) {
	e.removeEventListener(t, n, r);
}
var ko = /* @__PURE__ */ Symbol("_vei");
function Ao(e, t, n, r, i = null) {
	let a = e[ko] || (e[ko] = {}), o = a[t];
	if (r && o) o.value = r;
	else {
		let [n, s] = No(t);
		r ? Do(e, n, a[t] = Lo(r, i), s) : o && (Oo(e, n, o, s), a[t] = void 0);
	}
}
var jo = /(Once|Passive|Capture)$/, Mo = /^on:?(?:Once|Passive|Capture)$/;
function No(e) {
	let t, n;
	for (; (n = e.match(jo)) && !Mo.test(e);) t ||= {}, e = e.slice(0, e.length - n[1].length), t[n[1].toLowerCase()] = !0;
	return [e[2] === ":" ? e.slice(3) : A(e.slice(2)), t];
}
var Po = 0, Fo = /* @__PURE__ */ Promise.resolve(), Io = () => Po ||= (Fo.then(() => Po = 0), Date.now());
function Lo(e, t) {
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
				e && Xt(e, t, 5, a);
			}
		} else Xt(r, t, 5, [e]);
	};
	return n.value = e, n.attached = Io(), n;
}
var Ro = (e) => e.charCodeAt(0) === 111 && e.charCodeAt(1) === 110 && e.charCodeAt(2) > 96 && e.charCodeAt(2) < 123, zo = (e, t, n, r, i, s) => {
	let c = i === "svg";
	t === "class" ? fo(e, r, c) : t === "style" ? _o(e, n, r) : a(t) ? o(t) || Ao(e, t, n, r, s) : (t[0] === "." ? (t = t.slice(1), !0) : t[0] === "^" ? (t = t.slice(1), !1) : Bo(e, t, r, c)) ? (Eo(e, t, r), !e.tagName.includes("-") && (t === "value" || t === "checked" || t === "selected") && To(e, t, r, c, s, t !== "value")) : e._isVueCE && (Vo(e, t) || e._def.__asyncLoader && (/[A-Z]/.test(t) || !g(r))) ? Eo(e, O(t), r, s, t) : (t === "true-value" ? e._trueValue = r : t === "false-value" && (e._falseValue = r), To(e, t, r, c));
};
function Bo(e, t, n, r) {
	if (r) return !!(t === "innerHTML" || t === "textContent" || t in e && Ro(t) && h(n));
	if (t === "spellcheck" || t === "draggable" || t === "translate" || t === "autocorrect" || t === "sandbox" && e.tagName === "IFRAME" || t === "form" || t === "list" && e.tagName === "INPUT" || t === "type" && e.tagName === "TEXTAREA") return !1;
	if (t === "width" || t === "height") {
		let t = e.tagName;
		if (t === "IMG" || t === "VIDEO" || t === "CANVAS" || t === "SOURCE") return !1;
	}
	return Ro(t) && g(n) ? !1 : t in e;
}
function Vo(e, t) {
	let n = e._def.props;
	if (!n) return !1;
	let r = O(t);
	return Array.isArray(n) ? n.some((e) => O(e) === r) : Object.keys(n).some((e) => O(e) === r);
}
var Ho = (e) => {
	let t = e.props["onUpdate:modelValue"] || !1;
	return d(t) ? (e) => te(t, e) : t;
};
function Uo(e) {
	e.target.composing = !0;
}
function Wo(e) {
	let t = e.target;
	t.composing && (t.composing = !1, t.dispatchEvent(new Event("input")));
}
var Go = /* @__PURE__ */ Symbol("_assign");
function Ko(e, t, n) {
	return t && (e = e.trim()), n && (e = ne(e)), e;
}
var qo = {
	created(e, { modifiers: { lazy: t, trim: n, number: r } }, i) {
		e[Go] = Ho(i);
		let a = r || i.props && i.props.type === "number";
		Do(e, t ? "change" : "input", (t) => {
			t.target.composing || e[Go](Ko(e.value, n, a));
		}), (n || a) && Do(e, "change", () => {
			e.value = Ko(e.value, n, a);
		}), t || (Do(e, "compositionstart", Uo), Do(e, "compositionend", Wo), Do(e, "change", Wo));
	},
	mounted(e, { value: t }) {
		e.value = t ?? "";
	},
	beforeUpdate(e, { value: t, oldValue: n, modifiers: { lazy: r, trim: i, number: a } }, o) {
		if (e[Go] = Ho(o), e.composing) return;
		let s = (a || e.type === "number") && !/^0\d/.test(e.value) ? ne(e.value) : e.value, c = t ?? "";
		if (s === c) return;
		let l = e.getRootNode();
		(l instanceof Document || l instanceof ShadowRoot) && l.activeElement === e && e.type !== "range" && (r && t === n || i && e.value.trim() === c) || (e.value = c);
	}
}, Jo = {
	deep: !0,
	created(e, t, n) {
		e[Go] = Ho(n), Do(e, "change", () => {
			let t = e._modelValue, n = $o(e), r = e.checked, i = e[Go];
			if (d(t)) {
				let e = ce(t, n), a = e !== -1;
				if (r && !a) i(t.concat(n));
				else if (!r && a) {
					let n = [...t];
					n.splice(e, 1), i(n);
				}
			} else if (p(t)) {
				let e = new Set(t);
				r ? e.add(n) : e.delete(n), i(e);
			} else i(es(e, r));
		});
	},
	mounted: Yo,
	beforeUpdate(e, t, n) {
		e[Go] = Ho(n), Yo(e, t, n);
	}
};
function Yo(e, { value: t, oldValue: n }, r) {
	e._modelValue = t;
	let i;
	if (d(t)) i = ce(t, r.props.value) > -1;
	else if (p(t)) i = t.has(r.props.value);
	else {
		if (t === n) return;
		i = H(t, es(e, !0));
	}
	e.checked !== i && (e.checked = i);
}
var Xo = {
	created(e, { value: t }, n) {
		e.checked = H(t, n.props.value), e[Go] = Ho(n), Do(e, "change", () => {
			e[Go]($o(e));
		});
	},
	beforeUpdate(e, { value: t, oldValue: n }, r) {
		e[Go] = Ho(r), t !== n && (e.checked = H(t, r.props.value));
	}
}, Zo = {
	deep: !0,
	created(e, { value: t, modifiers: { number: n } }, r) {
		e._modelValue = t, Do(e, "change", () => {
			let t = Array.prototype.filter.call(e.options, (e) => e.selected).map((e) => n ? ne($o(e)) : $o(e));
			e[Go](e.multiple ? p(e._modelValue) ? new Set(t) : t : t[0]), e._assigning = !0, sn(() => {
				e._assigning = !1;
			});
		}), e[Go] = Ho(r);
	},
	mounted(e, { value: t }) {
		Qo(e, t);
	},
	beforeUpdate(e, { value: t }, n) {
		e._modelValue = t, e[Go] = Ho(n);
	},
	updated(e, { value: t }) {
		e._assigning || Qo(e, t);
	}
};
function Qo(e, t) {
	let n = e.multiple, r = d(t);
	if (!(n && !r && !p(t))) {
		for (let i = 0, a = e.options.length; i < a; i++) {
			let a = e.options[i], o = $o(a);
			if (n) if (r) {
				let e = typeof o;
				a.selected = e === "string" || e === "number" ? t.some((e) => String(e) === String(o)) : ce(t, o) > -1;
			} else a.selected = t.has(o);
			else if (H($o(a), t)) {
				e.selectedIndex !== i && (e.selectedIndex = i);
				return;
			}
		}
		!n && e.selectedIndex !== -1 && (e.selectedIndex = -1);
	}
}
function $o(e) {
	return "_value" in e ? e._value : e.value;
}
function es(e, t) {
	let n = t ? "_trueValue" : "_falseValue";
	return n in e ? e[n] : t;
}
var ts = {
	created(e, t, n) {
		rs(e, t, n, null, "created");
	},
	mounted(e, t, n) {
		rs(e, t, n, null, "mounted");
	},
	beforeUpdate(e, t, n, r) {
		rs(e, t, n, r, "beforeUpdate");
	},
	updated(e, t, n, r) {
		rs(e, t, n, r, "updated");
	}
};
function ns(e, t) {
	switch (e) {
		case "SELECT": return Zo;
		case "TEXTAREA": return qo;
		default: switch (t) {
			case "checkbox": return Jo;
			case "radio": return Xo;
			default: return qo;
		}
	}
}
function rs(e, t, n, r, i) {
	let a = ns(e.tagName, n.props && n.props.type)[i];
	a && a(e, t, n, r);
}
var is = [
	"ctrl",
	"shift",
	"alt",
	"meta"
], as = {
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
	exact: (e, t) => is.some((n) => e[`${n}Key`] && !t.includes(n))
}, os = (e, t) => {
	if (!e) return e;
	let n = e._withMods ||= {}, r = t.join(".");
	return n[r] || (n[r] = ((n, ...r) => {
		for (let e = 0; e < t.length; e++) {
			let r = as[t[e]];
			if (r && r(n, t)) return;
		}
		return e(n, ...r);
	}));
}, ss = {
	esc: "escape",
	space: " ",
	up: "arrow-up",
	left: "arrow-left",
	right: "arrow-right",
	down: "arrow-down",
	delete: "backspace"
}, cs = (e, t) => {
	let n = e._withKeys ||= {}, r = t.join(".");
	return n[r] || (n[r] = ((n) => {
		if (!("key" in n)) return;
		let r = A(n.key);
		if (t.some((e) => e === r || ss[e] === r)) return e(n);
	}));
}, ls = /* @__PURE__ */ s({ patchProp: zo }, Wa), us;
function ds() {
	return us ||= Ni(ls);
}
var fs = ((...e) => {
	let t = ds().createApp(...e), { mount: n } = t;
	return t.mount = (e) => {
		let r = ms(e);
		if (!r) return;
		let i = t._component;
		!h(i) && !i.render && !i.template && (i.template = r.innerHTML), r.nodeType === 1 && (r.textContent = "");
		let a = n(r, !1, ps(r));
		return r instanceof Element && (r.removeAttribute("v-cloak"), r.setAttribute("data-v-app", "")), a;
	}, t;
});
function ps(e) {
	if (e instanceof SVGElement) return "svg";
	if (typeof MathMLElement == "function" && e instanceof MathMLElement) return "mathml";
}
function ms(e) {
	return g(e) ? document.querySelector(e) : e;
}
//#endregion
//#region src/shared/api.ts
async function hs(e) {
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
async function gs(e) {
	return hs(await fetch(e, {
		credentials: "same-origin",
		headers: { Accept: "application/json" }
	}));
}
async function _s(e, t = {}) {
	return hs(await fetch(e, {
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
function vs(e) {
	return String(e ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll("\"", "&quot;").replaceAll("'", "&#039;");
}
function ys(e) {
	let t = String(e ?? "").trim(), n = t.toLowerCase();
	return n.startsWith("https://") || n.startsWith("http://") || n.startsWith("mailto:") || n.startsWith("tel:") || t.startsWith("/") || t.startsWith("#") ? t : "";
}
function bs(e) {
	let t = [], n = String(e ?? "");
	return n = n.replace(/`([^`\n]+)`/g, (e, n) => {
		let r = `@@TC_${t.length}@@`;
		return t.push(`<code>${vs(n)}</code>`), r;
	}), n = n.replace(/\[([^\]\n]+)\]\(([^)\s]+)\)/g, (e, n, r) => {
		let i = `@@TC_${t.length}@@`, a = ys(r);
		return t.push(a ? `<a href="${vs(a)}" target="_blank" rel="noopener noreferrer">${vs(n)}</a>` : `${vs(n)} (${vs(r)})`), i;
	}), n = vs(n).replace(/\*\*([^*\n][^*\n]*?)\*\*/g, "<strong>$1</strong>").replace(/\*([^*\n][^*\n]*?)\*/g, "<em>$1</em>"), t.forEach((e, t) => {
		n = n.replaceAll(`@@TC_${t}@@`, e);
	}), n;
}
function xs(e) {
	let t = String(e ?? "").replace(/\r\n?/g, "\n").split("\n"), n = [], r = [], i = "", a = [], o = !1, s = "", c = [], l = () => {
		r.length && (n.push(`<p>${r.map((e) => bs(e.trim())).join("<br />")}</p>`), r = []);
	}, u = () => {
		i && a.length && n.push(`<${i}>${a.map((e) => `<li>${bs(e)}</li>`).join("")}</${i}>`), i = "", a = [];
	}, d = () => {
		if (!o) return;
		let e = s.replace(/[^A-Za-z0-9_+\-]/g, "");
		n.push(`<pre><code${e ? ` class="language-${e}"` : ""}>${vs(c.join("\n"))}</code></pre>`), o = !1, s = "", c = [];
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
			n.push(`<h${e}>${bs(p[2])}</h${e}>`);
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
			l(), u(), n.push(`<blockquote>${bs(f.slice(2))}</blockquote>`);
			return;
		}
		i && u(), r.push(f);
	}), l(), u(), d(), n.join("") || `<p>${bs(e)}</p>`;
}
//#endregion
//#region src/chat/components/ChatMessage.vue?vue&type=script&setup=true&lang.ts
var Ss = {
	key: 0,
	class: "chat-avatar"
}, Cs = ["src", "alt"], ws = {
	key: 1,
	class: "chat-avatar-fallback assistant"
}, Ts = { class: "role" }, Es = ["aria-label"], Ds = { class: "chat-typing-label" }, Os = {
	key: 1,
	class: "bubble-body"
}, ks = ["innerHTML"], As = {
	key: 3,
	class: "bubble-body"
}, js = ["src", "alt"], Ms = {
	key: 5,
	class: "chat-media-wrap"
}, Ns = ["src"], Ps = { class: "chat-file-meta" }, Fs = ["href", "download"], Is = {
	key: 6,
	class: "chat-media-wrap"
}, Ls = ["src"], Rs = { class: "chat-file-meta" }, zs = ["href", "download"], Bs = {
	key: 7,
	class: "chat-file-card"
}, Vs = { class: "chat-file-meta" }, Hs = ["href", "download"], Us = { key: 8 }, Ws = {
	key: 1,
	class: "chat-avatar"
}, Gs = ["src", "alt"], Ks = {
	key: 1,
	class: "chat-avatar-fallback user"
}, qs = /* @__PURE__ */ ar({
	__name: "ChatMessage",
	props: {
		message: {},
		profile: {},
		filesEndpoint: {}
	},
	emits: ["mediaReady"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = $(() => String(n.message.role || "assistant").toLowerCase() === "user" ? "user" : "assistant"), a = $(() => i.value === "user"), o = $(() => {
			let e = String(n.profile.tater_first_name || n.profile.tater_name || "Tater").trim() || "Tater", t = String(n.profile.tater_last_name || "Totterson").trim();
			return String(n.profile.tater_full_name || [e, t].filter(Boolean).join(" ") || "Tater Totterson").trim();
		}), s = $(() => a.value ? String(n.message.username || n.profile.username || "User") : o.value), c = $(() => String(a.value ? n.profile.user_avatar || "" : n.profile.tater_avatar || "")), l = $(() => (s.value.match(/[A-Za-z0-9]/)?.[0] || (a.value ? "U" : "T")).toUpperCase()), u = $(() => {
			let e = n.message.content;
			return e && typeof e == "object" ? e : null;
		}), d = $(() => String(u.value?.marker || "").trim().toLowerCase()), f = $(() => String(u.value?.type || "").trim().toLowerCase()), p = $(() => String(u.value?.name || "attachment").trim() || "attachment"), m = $(() => {
			let e = String(u.value?.mimetype || "application/octet-stream").trim();
			return /^[A-Za-z0-9.+-]+\/[A-Za-z0-9.+-]+$/.test(e) ? e : "application/octet-stream";
		}), h = $(() => {
			let e = Number(u.value?.size || 0);
			return !Number.isFinite(e) || e <= 0 ? "" : e < 1024 ? `${Math.round(e)} B` : e < 1024 ** 2 ? `${(e / 1024).toFixed(1)} KB` : e < 1024 ** 3 ? `${(e / 1024 ** 2).toFixed(1)} MB` : `${(e / 1024 ** 3).toFixed(1)} GB`;
		}), g = $(() => {
			let e = u.value;
			if (!e) return "";
			let t = String(e.data_b64 || "").trim();
			if (t) return `data:${m.value};base64,${t}`;
			let r = String(e.url || e.src || e.href || "").trim();
			if (r) {
				let e = ys(r);
				if (!e || /^(mailto:|tel:|#)/i.test(e)) return "";
				if (e.startsWith("/")) {
					let t = n.filesEndpoint.indexOf("/api/chat/files");
					return `${t >= 0 ? n.filesEndpoint.slice(0, t) : ""}${e}`;
				}
				return e;
			}
			let i = String(e.id || e.file_id || "").trim();
			return i ? `${n.filesEndpoint}/${encodeURIComponent(i)}?mimetype=${encodeURIComponent(m.value)}` : "";
		}), _ = $(() => typeof n.message.content == "string" ? n.message.content : ""), v = $(() => String(u.value?.content || "Working on it…")), y = $(() => {
			try {
				return JSON.stringify(u.value, null, 2);
			} catch {
				return String(u.value ?? "");
			}
		});
		return (t, n) => (J(), Y("article", { class: z(["chat-row", [i.value, { "typing-indicator": d.value === "typing" }]]) }, [
			a.value ? Q("", !0) : (J(), Y("div", Ss, [c.value ? (J(), Y("img", {
				key: 0,
				class: "chat-avatar-img",
				src: c.value,
				alt: `${s.value} avatar`
			}, null, 8, Cs)) : (J(), Y("div", ws, U(l.value), 1))])),
			X("div", { class: z(["bubble", i.value]) }, [X("div", Ts, U(s.value), 1), d.value === "typing" ? (J(), Y("div", {
				key: 0,
				class: "bubble-body chat-typing-body",
				"aria-label": `${s.value} is typing`
			}, [X("span", Ds, U(s.value) + " is typing", 1), n[6] ||= X("span", {
				class: "chat-typing-dots",
				"aria-hidden": "true"
			}, [
				X("span"),
				X("span"),
				X("span")
			], -1)], 8, Es)) : d.value === "plugin_wait" ? (J(), Y("div", Os, U(v.value), 1)) : typeof e.message.content == "string" && !a.value ? (J(), Y("div", {
				key: 2,
				class: "bubble-body markdown",
				innerHTML: Rt(xs)(_.value)
			}, null, 8, ks)) : typeof e.message.content == "string" ? (J(), Y("div", As, U(_.value), 1)) : f.value === "image" && g.value ? (J(), Y("img", {
				key: 4,
				class: "chat-media-image",
				src: g.value,
				alt: p.value,
				onLoad: n[0] ||= (e) => r("mediaReady"),
				onError: n[1] ||= (e) => r("mediaReady")
			}, null, 40, js)) : f.value === "audio" && g.value ? (J(), Y("div", Ms, [
				X("audio", {
					controls: "",
					preload: "metadata",
					src: g.value,
					onLoadedmetadata: n[2] ||= (e) => r("mediaReady"),
					onError: n[3] ||= (e) => r("mediaReady")
				}, null, 40, Ns),
				X("div", Ps, U(p.value), 1),
				X("a", {
					class: "tv-button tc-download",
					href: g.value,
					download: p.value
				}, "Download audio", 8, Fs)
			])) : f.value === "video" && g.value ? (J(), Y("div", Is, [
				X("video", {
					controls: "",
					preload: "metadata",
					src: g.value,
					class: "chat-media-video",
					onLoadedmetadata: n[4] ||= (e) => r("mediaReady"),
					onError: n[5] ||= (e) => r("mediaReady")
				}, null, 40, Ls),
				X("div", Rs, U(p.value), 1),
				X("a", {
					class: "tv-button tc-download",
					href: g.value,
					download: p.value
				}, "Download video", 8, zs)
			])) : f.value === "file" && g.value ? (J(), Y("div", Bs, [X("div", Vs, [Z(U(p.value), 1), h.value ? (J(), Y(q, { key: 0 }, [Z(" (" + U(h.value) + ")", 1)], 64)) : Q("", !0)]), X("a", {
				class: "tv-button tc-download",
				href: g.value,
				download: p.value
			}, "Download file", 8, Hs)])) : (J(), Y("pre", Us, U(y.value), 1))], 2),
			a.value ? (J(), Y("div", Ws, [c.value ? (J(), Y("img", {
				key: 0,
				class: "chat-avatar-img",
				src: c.value,
				alt: `${s.value} avatar`
			}, null, 8, Gs)) : (J(), Y("div", Ks, U(l.value), 1))])) : Q("", !0)
		], 2));
	}
}), Js = { class: "tater-vue-surface tc-chat" }, Ys = { class: "tv-panel chat-feed-card tc-feed-card" }, Xs = {
	key: 0,
	class: "tc-empty-chat"
}, Zs = { class: "tc-empty-avatar" }, Qs = ["data-chat-stream-job"], $s = {
	key: 0,
	class: "tc-attachment-tray"
}, ec = { class: "tc-attachment-list" }, tc = ["title", "onClick"], nc = {
	key: 1,
	class: "tc-job-strip",
	"aria-label": "Active chat jobs"
}, rc = { key: 0 }, ic = { class: "message-box chat-composer-card tc-composer-card" }, ac = {
	class: "chat-composer",
	role: "group",
	"aria-label": "Chat composer"
}, oc = { class: "chat-composer-bar" }, sc = {
	class: "chat-composer-btn chat-composer-attach",
	title: "Attach files",
	"aria-label": "Attach files"
}, cc = ["placeholder"], lc = [
	"disabled",
	"title",
	"aria-label"
], uc = {
	key: 2,
	class: "chat-speed-stats tc-speed-stats"
}, dc = {
	class: "chat-live-status tc-live-status",
	"aria-live": "polite"
}, fc = /* @__PURE__ */ ar({
	__name: "ChatApp",
	props: {
		state: {},
		options: {}
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ G(null), r = /* @__PURE__ */ G(null), i = /* @__PURE__ */ G(null), a = /* @__PURE__ */ G(""), o = /* @__PURE__ */ G([]), s = /* @__PURE__ */ G(!1), c = /* @__PURE__ */ G(""), l = /* @__PURE__ */ G(String(t.options.sessionId || "")), u = /* @__PURE__ */ G([]), d = /* @__PURE__ */ G({}), f = /* @__PURE__ */ G({ ...t.options.initialJobs || {} }), p = /* @__PURE__ */ G(!0), m = {}, h = {}, g = $(() => t.state.profile || {}), _ = $(() => Array.isArray(t.state.messages) ? t.state.messages : []), v = $(() => {
			let e = String(g.value.tater_first_name || g.value.tater_name || "Tater").trim() || "Tater", t = String(g.value.tater_last_name || "Totterson").trim();
			return String(g.value.tater_full_name || [e, t].filter(Boolean).join(" ") || "Tater Totterson").trim();
		}), y = $(() => Object.entries(f.value).filter(([, e]) => !!e)), b = $(() => y.value.length), x = $(() => Object.entries(d.value).filter(([, e]) => !!e)), S = $(() => ({
			role: "assistant",
			content: { marker: "typing" }
		})), C = $(() => {
			if (!b.value) return c.value;
			let e = /* @__PURE__ */ new Map();
			y.value.forEach(([, t]) => {
				let n = String(t.current_tool || "").trim();
				if (!n) return;
				let r = n.toLowerCase(), i = e.get(r) || {
					label: n,
					count: 0
				};
				i.count += 1, e.set(r, i);
			});
			let t = [...e.values()].sort((e, t) => t.count - e.count).slice(0, 3).map((e) => `${e.count} using ${e.label}`);
			return `${b.value} ${b.value === 1 ? "job" : "jobs"} running${t.length ? ` • ${t.join(" • ")}` : ""}`;
		}), w = $(() => {
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
		function T(e, n = "success") {
			t.options.onToast?.(e, n);
		}
		function E(e, n) {
			let r = e instanceof Error ? e.message : n;
			return c.value = r, t.options.onRequestError?.(r), r;
		}
		function D() {
			t.options.onJobsChange?.({ ...f.value });
		}
		function O(e) {
			m[e]?.close(), delete m[e];
		}
		function k(e) {
			h[e] && window.clearTimeout(h[e]), delete h[e];
		}
		function A(e, t) {
			let n = f.value[e] || {};
			f.value = {
				...f.value,
				[e]: {
					...n,
					...t,
					status: String(t.status || n.status || "running").toLowerCase(),
					updated_at: Date.now()
				}
			}, D();
		}
		function ee(e) {
			let t = { ...f.value };
			delete t[e], f.value = t, D();
		}
		function j() {
			p.value = !0, sn(() => {
				n.value && (n.value.scrollTop = n.value.scrollHeight);
			});
		}
		function M() {
			p.value && j();
		}
		function te() {
			let e = n.value;
			e && (p.value = e.scrollHeight - e.scrollTop - e.clientHeight < 120);
		}
		async function N() {
			let e = await gs(t.options.endpoints.history);
			t.state.messages = Array.isArray(e.messages) ? e.messages : [], u.value = [];
		}
		async function ne() {
			try {
				t.state.stats = await gs(t.options.endpoints.stats);
			} catch {
				t.state.stats = {
					enabled: !1,
					stats: null
				};
			}
		}
		async function re(e, n, r = []) {
			if (!f.value[e]) return;
			O(e), k(e), ee(e);
			let i = { ...d.value };
			delete i[e], d.value = i;
			try {
				await N();
			} catch (e) {
				r.length ? t.state.messages = [...t.state.messages, ...r.map((e) => ({
					role: "assistant",
					username: "assistant",
					content: e
				}))] : E(e, "Chat history refresh failed.");
			}
			await ne(), t.options.onHealthRefresh?.(), c.value = n, j();
		}
		function P(e, t) {
			let n = String(t.status || "running").trim().toLowerCase();
			if (n === "done") {
				re(e, "Complete.", Array.isArray(t.responses) ? t.responses : []);
				return;
			}
			if (n === "error") {
				re(e, `Job failed: ${String(t.error || "unknown error")}`);
				return;
			}
			A(e, {
				status: n || "running",
				current_tool: String(t.current_tool || "").trim(),
				task_name: String(t.task_name || f.value[e]?.task_name || "").trim()
			});
		}
		function F(e, n) {
			k(e), f.value[e] && (h[e] = window.setTimeout(async () => {
				if (f.value[e]) {
					try {
						P(e, await gs(`${t.options.endpoints.jobs}/${encodeURIComponent(e)}`));
					} catch (e) {
						t.options.onRequestError?.(e instanceof Error ? e.message : "Chat job polling failed.");
					}
					f.value[e] && F(e, 1200);
				}
			}, Math.max(250, n ?? (t.options.isIngress ? 900 : 2e3))));
		}
		function I(e) {
			try {
				return JSON.parse(String(e.data || "{}"));
			} catch {
				return {};
			}
		}
		function L(e, n = {}) {
			if (!e || (A(e, {
				status: "queued",
				...n
			}), O(e), F(e), typeof EventSource != "function")) return;
			let r = new EventSource(`${t.options.endpoints.jobs}/${encodeURIComponent(e)}/events`);
			m[e] = r, r.addEventListener("status", (t) => P(e, I(t))), r.addEventListener("tool", (t) => {
				let n = I(t);
				A(e, {
					status: "running",
					current_tool: String(n.current_tool || "tool"),
					task_name: String(n.task_name || f.value[e]?.task_name || "")
				});
			}), r.addEventListener("waiting", (e) => {
				let t = String(I(e).wait_text || "").trim();
				t && (u.value = [...u.value, {
					role: "assistant",
					content: {
						marker: "plugin_wait",
						content: t
					}
				}], M());
			}), r.addEventListener("response_chunk", (t) => {
				let n = String(I(t).chunk || "");
				n && (d.value = {
					...d.value,
					[e]: String(d.value[e] || "") + n
				}, M());
			}), r.addEventListener("done", (t) => {
				let n = I(t);
				re(e, "Complete.", Array.isArray(n.responses) ? n.responses : []);
			}), r.addEventListener("job_error", (t) => {
				re(e, `Job failed: ${String(I(t).error || "unknown error")}`);
			}), r.onerror = () => O(e);
		}
		function ie(e) {
			return e < 1024 ? `${e} B` : e < 1024 ** 2 ? `${(e / 1024).toFixed(1)} KB` : `${(e / 1024 ** 2).toFixed(1)} MB`;
		}
		function ae(e) {
			let t = e.target, n = Array.from(t.files || []), r = Number(g.value.attach_max_mb_each || 0) * 1024 ** 2, i = Number(g.value.attach_max_mb_total || 0) * 1024 ** 2, a = [], s = 0;
			for (let e of n) {
				if (r > 0 && e.size > r) {
					T(`${e.name} is larger than the ${g.value.attach_max_mb_each} MB attachment limit.`, "error");
					continue;
				}
				if (i > 0 && s + e.size > i) {
					T(`Attachments exceed the ${g.value.attach_max_mb_total} MB total limit.`, "error");
					break;
				}
				a.push(e), s += e.size;
			}
			o.value = a, t.value = "";
		}
		function R(e) {
			o.value = o.value.filter((t, n) => n !== e);
		}
		function z() {
			o.value = [], i.value && (i.value.value = "");
		}
		function B(e) {
			return new Promise((t, n) => {
				let r = new FileReader();
				r.onload = () => t(String(r.result || "")), r.onerror = () => n(/* @__PURE__ */ Error(`Could not read ${e.name}.`)), r.readAsDataURL(e);
			});
		}
		async function V() {
			if (s.value) return;
			let e = a.value.trim(), n = [...o.value];
			if (!e && !n.length) {
				c.value = "Enter a message or attach files first.";
				return;
			}
			s.value = !0, a.value = "", z(), se(), c.value = n.length ? "Preparing attachments…" : "Queueing chat job…", j();
			try {
				let r = [];
				for (let e of n) r.push({
					name: e.name || "attachment",
					mimetype: e.type || "application/octet-stream",
					data_url: await B(e)
				});
				let i = await _s(t.options.endpoints.jobs, {
					message: e,
					session_id: l.value,
					attachments: r
				}), a = String(i.session_id || "").trim();
				a && (l.value = a, t.options.onSessionChange?.(a));
				let o = String(i.job_id || "").trim();
				if (!o) throw Error("Backend did not return a job id.");
				await N(), L(o, {
					status: "queued",
					task_name: String(i.task_name || "")
				}), c.value = i.task_name ? `Job queued: ${i.task_name}` : "Job queued…", t.options.onHealthRefresh?.(), j();
			} catch (e) {
				T(`Chat failed: ${E(e, "Chat failed.")}`, "error");
			} finally {
				s.value = !1;
			}
		}
		function oe(e) {
			e.key === "Enter" && !e.shiftKey && !e.ctrlKey && !e.altKey && !e.metaKey && !e.isComposing && (e.preventDefault(), V());
		}
		function se() {
			sn(() => {
				let e = r.value;
				e && (e.style.height = "auto", e.style.height = `${Math.min(Math.max(e.scrollHeight, 44), 180)}px`);
			});
		}
		return En([
			() => _.value.length,
			() => u.value.length,
			() => x.value.map(([e, t]) => `${e}:${t.length}`).join("|"),
			b
		], M), En(a, se), br(() => {
			Object.entries(f.value).forEach(([e, t]) => L(e, t)), j();
		}), Cr(() => {
			Object.keys(m).forEach(O), Object.keys(h).forEach(k);
		}), (t, c) => (J(), Y("div", Js, [X("section", Ys, [
			X("div", {
				ref_key: "feed",
				ref: n,
				class: "chat-log tc-chat-log",
				onScroll: te
			}, [
				!_.value.length && !u.value.length && !x.value.length ? (J(), Y("div", Xs, [
					X("div", Zs, U(v.value.charAt(0)), 1),
					X("h2", null, "Talk to " + U(v.value), 1),
					c[1] ||= X("p", null, "Ask a question, control your home, or attach something for Tater to inspect.", -1)
				])) : Q("", !0),
				(J(!0), Y(q, null, K(_.value, (t, n) => (J(), ea(qs, {
					key: t.id || `history-${n}`,
					message: t,
					profile: g.value,
					"files-endpoint": e.options.endpoints.files,
					onMediaReady: M
				}, null, 8, [
					"message",
					"profile",
					"files-endpoint"
				]))), 128)),
				(J(!0), Y(q, null, K(u.value, (t, n) => (J(), ea(qs, {
					key: `ephemeral-${n}`,
					message: t,
					profile: g.value,
					"files-endpoint": e.options.endpoints.files
				}, null, 8, [
					"message",
					"profile",
					"files-endpoint"
				]))), 128)),
				(J(!0), Y(q, null, K(x.value, ([t, n]) => (J(), Y("div", {
					key: t,
					"data-chat-stream-job": t,
					"aria-live": "polite",
					"aria-busy": "true"
				}, [aa(qs, {
					message: {
						role: "assistant",
						content: n
					},
					profile: g.value,
					"files-endpoint": e.options.endpoints.files
				}, null, 8, [
					"message",
					"profile",
					"files-endpoint"
				])], 8, Qs))), 128)),
				b.value ? (J(), ea(qs, {
					key: 1,
					message: S.value,
					profile: g.value,
					"files-endpoint": e.options.endpoints.files
				}, null, 8, [
					"message",
					"profile",
					"files-endpoint"
				])) : Q("", !0)
			], 544),
			o.value.length ? (J(), Y("div", $s, [X("div", ec, [(J(!0), Y(q, null, K(o.value, (e, t) => (J(), Y("button", {
				key: `${e.name}-${e.size}-${t}`,
				type: "button",
				class: "tc-attachment-chip",
				title: `Remove ${e.name}`,
				onClick: (e) => R(t)
			}, [
				X("span", null, U(e.name), 1),
				X("small", null, U(ie(e.size)), 1),
				c[2] ||= X("b", { "aria-hidden": "true" }, "×", -1)
			], 8, tc))), 128))]), X("button", {
				type: "button",
				class: "tc-clear-files",
				onClick: z
			}, "Clear all")])) : Q("", !0),
			y.value.length ? (J(), Y("div", nc, [(J(!0), Y(q, null, K(y.value, ([e, t]) => (J(), Y("span", { key: e }, [
				c[3] ||= X("i", null, null, -1),
				Z(U(t.task_name || "Tater is working"), 1),
				t.current_tool ? (J(), Y("small", rc, U(t.current_tool), 1)) : Q("", !0)
			]))), 128))])) : Q("", !0),
			X("div", ic, [X("div", ac, [X("div", oc, [
				X("label", sc, [X("input", {
					ref_key: "fileInput",
					ref: i,
					class: "tc-file-input",
					type: "file",
					multiple: "",
					onChange: ae
				}, null, 544), c[4] ||= X("span", {
					class: "chat-composer-icon chat-composer-plus",
					"aria-hidden": "true"
				}, "+", -1)]),
				bn(X("textarea", {
					ref_key: "composer",
					ref: r,
					"onUpdate:modelValue": c[0] ||= (e) => a.value = e,
					class: "chat-composer-input",
					rows: "1",
					placeholder: `Message ${v.value}…`,
					onKeydown: oe
				}, null, 40, cc), [[qo, a.value]]),
				X("button", {
					type: "button",
					class: "chat-composer-send",
					disabled: s.value,
					title: s.value ? "Preparing message" : "Send message",
					"aria-label": s.value ? "Preparing message" : "Send message",
					onClick: V
				}, [...c[5] ||= [X("span", {
					class: "chat-composer-icon chat-composer-send-arrow",
					"aria-hidden": "true"
				}, "➤", -1)]], 8, lc)
			])])]),
			w.value ? (J(), Y("div", uc, U(w.value), 1)) : Q("", !0),
			X("div", dc, U(C.value), 1)
		])]));
	}
});
//#endregion
//#region src/music/api.ts
async function pc(e) {
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
async function mc(e) {
	return pc(await fetch(e, {
		headers: { Accept: "application/json" },
		credentials: "same-origin"
	}));
}
async function hc(e, t, n) {
	return pc(await fetch(e, {
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
//#region src/music/components/DynamicField.vue?vue&type=script&setup=true&lang.ts
var gc = ["checked", "disabled"], _c = { key: 0 }, vc = { class: "tm-option-grid" }, yc = [
	"checked",
	"disabled",
	"onChange"
], bc = { class: "tm-option-copy" }, xc = { key: 0 }, Sc = { key: 0 }, Cc = ["value", "disabled"], wc = ["value"], Tc = { key: 0 }, Ec = { class: "tm-range-row" }, Dc = [
	"value",
	"min",
	"max",
	"step",
	"disabled"
], Oc = [
	"value",
	"placeholder",
	"required",
	"disabled"
], kc = [
	"type",
	"value",
	"placeholder",
	"required",
	"disabled",
	"min",
	"max",
	"step"
], Ac = { key: 2 }, jc = /* @__PURE__ */ ar({
	__name: "DynamicField",
	props: {
		field: {},
		modelValue: {},
		compact: { type: Boolean }
	},
	emits: ["update:modelValue", "change"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = $(() => String(n.field.type || "text").toLowerCase()), a = $(() => !!(n.field.disabled || n.field.read_only)), o = $(() => String(n.modelValue ?? "")), s = $(() => Number(n.modelValue ?? 0)), c = $(() => new Set((Array.isArray(n.modelValue) ? n.modelValue : [n.modelValue]).map((e) => String(e ?? "")).filter(Boolean)));
		function l(e) {
			return String(e && typeof e == "object" ? e.value ?? e.id ?? e.key ?? e.label ?? "" : e ?? "");
		}
		function u(e) {
			return e && typeof e == "object" ? [
				e.label,
				e.title,
				e.name,
				e.friendly_name,
				e.description,
				e.meta,
				l(e)
			].map((e) => String(e ?? "").trim()).find(Boolean) || "Unnamed player" : String(e ?? "").trim() || "Unnamed player";
		}
		function d(e) {
			if (!e || typeof e != "object") return "";
			let t = u(e);
			return [
				e.description,
				e.meta,
				e.room,
				e.area
			].map((e) => String(e ?? "").trim()).find((e) => !!e && e !== t) || "";
		}
		function f(e) {
			let t = e.target, n;
			n = i.value === "checkbox" ? t.checked : i.value === "number" || i.value === "range" ? Number(t.value) : t.value, r("update:modelValue", n);
		}
		function p(e) {
			let t = e.target, n = i.value === "checkbox" ? t.checked : i.value === "number" || i.value === "range" ? Number(t.value) : t.value;
			r("update:modelValue", n), r("change", n);
		}
		function m(e, t) {
			let n = new Set(c.value);
			t ? n.add(e) : n.delete(e), r("update:modelValue", Array.from(n));
		}
		return (t, n) => i.value === "checkbox" ? (J(), Y("label", {
			key: 0,
			class: z(["tm-field tm-checkbox", { compact: e.compact }])
		}, [X("input", {
			type: "checkbox",
			checked: !!e.modelValue,
			disabled: a.value,
			onChange: f
		}, null, 40, gc), X("span", null, [X("strong", null, U(e.field.label || e.field.key), 1), e.field.description ? (J(), Y("small", _c, U(e.field.description), 1)) : Q("", !0)])], 2)) : i.value === "multiselect" ? (J(), Y("fieldset", {
			key: 1,
			class: z(["tm-field tm-multiselect", { compact: e.compact }])
		}, [
			X("legend", null, U(e.field.label || e.field.key), 1),
			X("div", vc, [(J(!0), Y(q, null, K(e.field.options || [], (e) => (J(), Y("label", {
				key: l(e),
				class: "tm-option"
			}, [X("input", {
				type: "checkbox",
				checked: c.value.has(l(e)),
				disabled: a.value || !l(e),
				onChange: (t) => m(l(e), t.target.checked)
			}, null, 40, yc), X("span", bc, [X("strong", null, U(u(e)), 1), d(e) ? (J(), Y("small", xc, U(d(e)), 1)) : Q("", !0)])]))), 128))]),
			e.field.description ? (J(), Y("small", Sc, U(e.field.description), 1)) : Q("", !0)
		], 2)) : i.value === "select" ? (J(), Y("label", {
			key: 2,
			class: z(["tm-field", { compact: e.compact }])
		}, [
			X("span", null, U(e.field.label || e.field.key), 1),
			X("select", {
				value: o.value,
				disabled: a.value,
				onChange: f
			}, [(J(!0), Y(q, null, K(e.field.options || [], (e) => (J(), Y("option", {
				key: l(e),
				value: l(e)
			}, U(u(e)), 9, wc))), 128))], 40, Cc),
			e.field.description ? (J(), Y("small", Tc, U(e.field.description), 1)) : Q("", !0)
		], 2)) : i.value === "range" ? (J(), Y("label", {
			key: 3,
			class: z(["tm-field tm-range", { compact: e.compact }])
		}, [X("span", null, U(e.field.label || e.field.key), 1), X("div", Ec, [X("input", {
			type: "range",
			value: s.value,
			min: e.field.min ?? 0,
			max: e.field.max ?? 100,
			step: e.field.step ?? 1,
			disabled: a.value,
			onInput: f,
			onChange: p
		}, null, 40, Dc), X("output", null, U(s.value) + U(e.field.suffix || ""), 1)])], 2)) : (J(), Y("label", {
			key: 4,
			class: z(["tm-field", { compact: e.compact }])
		}, [
			X("span", null, U(e.field.label || e.field.key), 1),
			i.value === "textarea" || i.value === "multiline" ? (J(), Y("textarea", {
				key: 0,
				value: o.value,
				placeholder: e.field.placeholder,
				required: e.field.required,
				disabled: a.value,
				onInput: f
			}, null, 40, Oc)) : (J(), Y("input", {
				key: 1,
				type: i.value === "password" ? "password" : i.value === "number" ? "number" : "text",
				value: e.modelValue,
				placeholder: e.field.placeholder,
				required: e.field.required,
				disabled: a.value,
				min: e.field.min,
				max: e.field.max,
				step: e.field.step,
				onInput: f
			}, null, 40, kc)),
			e.field.description ? (J(), Y("small", Ac, U(e.field.description), 1)) : Q("", !0)
		], 2));
	}
}), Mc = { class: "tm-library" }, Nc = {
	class: "tm-subtabs",
	"aria-label": "Browse music library"
}, Pc = ["onClick"], Fc = { class: "tm-search-controls" }, Ic = ["disabled"], Lc = {
	key: 0,
	class: "tm-library-grid"
}, Rc = { class: "tm-library-art" }, zc = ["src", "alt"], Bc = {
	key: 1,
	"aria-hidden": "true"
}, Vc = [
	"disabled",
	"aria-label",
	"onClick"
], Hc = { class: "tm-library-copy" }, Uc = ["title"], Wc = {
	key: 1,
	class: "tm-empty"
}, Gc = {
	key: 2,
	class: "tm-pagination",
	"aria-label": "Library pages"
}, Kc = ["disabled"], qc = ["disabled"], Jc = /* @__PURE__ */ ar({
	__name: "LibraryBrowser",
	props: {
		groups: {},
		items: {},
		busy: { type: Function },
		run: { type: Function }
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ G(t.groups[0]?.key || "search"), r = /* @__PURE__ */ G({}), i = /* @__PURE__ */ G({});
		En(() => t.groups, (e) => {
			e.some((e) => e.key === n.value) || (n.value = e[0]?.key || "search");
		}, { deep: !0 });
		let a = $(() => t.groups.find((e) => e.key === n.value)), o = $(() => {
			let e = a.value?.item_group || a.value?.key;
			return t.items.filter((t) => t.group === e);
		}), s = $(() => o.value[0]), c = $(() => Math.max(0, Number(a.value?.page_size || 0))), l = $(() => Math.max(1, r.value[n.value] || 1)), u = $(() => c.value ? Math.max(1, Math.ceil(o.value.length / c.value)) : 1), d = $(() => {
			if (!c.value) return o.value;
			let e = (Math.min(l.value, u.value) - 1) * c.value;
			return o.value.slice(e, e + c.value);
		});
		En(s, (e) => {
			if (!e) return;
			let t = { ...i.value };
			for (let n of e.fields || []) n.key in t || (t[n.key] = n.value);
			i.value = t;
		}, { immediate: !0 });
		function f(e) {
			r.value = {
				...r.value,
				[n.value]: Math.max(1, Math.min(e, u.value))
			};
		}
		function p(e, t) {
			i.value = {
				...i.value,
				[e.key]: t
			};
		}
		async function m() {
			let e = s.value;
			e?.run_action && await t.run(e.run_action, {
				id: e.id,
				values: i.value
			}, `item:${e.id}`);
		}
		async function h(e) {
			e.run_action && await t.run(e.run_action, {
				id: e.id,
				values: {}
			}, `item:${e.id}`);
		}
		return (t, r) => (J(), Y("section", Mc, [X("nav", Nc, [(J(!0), Y(q, null, K(e.groups, (e) => (J(), Y("button", {
			key: e.key,
			type: "button",
			class: z({ active: n.value === e.key }),
			onClick: (t) => n.value = e.key
		}, U(e.label || e.key), 11, Pc))), 128))]), a.value?.key === "search" && s.value ? (J(), Y("form", {
			key: 0,
			class: "tm-search",
			onSubmit: os(m, ["prevent"])
		}, [X("div", null, [
			r[2] ||= X("div", { class: "tm-eyebrow" }, "Search across your connected library", -1),
			X("h3", null, U(s.value.title || "Find music"), 1),
			X("p", null, U(s.value.subtitle), 1)
		]), X("div", Fc, [(J(!0), Y(q, null, K(s.value.fields || [], (e) => (J(), ea(jc, {
			key: e.key,
			field: e,
			"model-value": i.value[e.key],
			compact: "",
			"onUpdate:modelValue": (t) => p(e, t)
		}, null, 8, [
			"field",
			"model-value",
			"onUpdate:modelValue"
		]))), 128)), X("button", {
			type: "submit",
			class: "tm-button primary",
			disabled: e.busy(`item:${s.value.id}`)
		}, U(s.value.run_label || "Play Search"), 9, Ic)])], 32)) : (J(), Y(q, { key: 1 }, [d.value.length ? (J(), Y("div", Lc, [(J(!0), Y(q, null, K(d.value, (t) => (J(), Y("article", {
			key: t.id,
			class: "tm-library-card"
		}, [X("div", Rc, [t.hero_image_src ? (J(), Y("img", {
			key: 0,
			src: t.hero_image_src,
			alt: t.hero_image_alt || "",
			loading: "lazy"
		}, null, 8, zc)) : (J(), Y("span", Bc, "♫")), t.run_action ? (J(), Y("button", {
			key: 2,
			type: "button",
			disabled: e.busy(`item:${t.id}`),
			"aria-label": `${t.run_label || "Play"} ${t.title || ""}`,
			onClick: (e) => h(t)
		}, " ▶ ", 8, Vc)) : Q("", !0)]), X("div", Hc, [X("strong", { title: t.title }, U(t.title || "Untitled"), 9, Uc), X("small", null, U(t.subtitle), 1)])]))), 128))])) : (J(), Y("div", Wc, U(a.value?.empty_message || "Nothing is available here yet."), 1)), u.value > 1 ? (J(), Y("div", Gc, [
			X("button", {
				type: "button",
				disabled: l.value <= 1,
				onClick: r[0] ||= (e) => f(l.value - 1)
			}, "Previous", 8, Kc),
			X("span", null, "Page " + U(Math.min(l.value, u.value)) + " of " + U(u.value), 1),
			X("button", {
				type: "button",
				disabled: l.value >= u.value,
				onClick: r[1] ||= (e) => f(l.value + 1)
			}, "Next", 8, qc)
		])) : Q("", !0)], 64))]));
	}
}), Yc = { class: "tm-queue" }, Xc = { class: "tm-queue-summary-actions" }, Zc = ["checked", "disabled"], Qc = {
	key: 0,
	class: "tm-track-scroll",
	role: "listbox",
	"aria-label": "Current track list"
}, $c = [
	"disabled",
	"aria-current",
	"title",
	"onDblclick"
], el = { class: "tm-track-position" }, tl = ["src", "alt"], nl = {
	key: 1,
	class: "tm-track-art placeholder",
	"aria-hidden": "true"
}, rl = { class: "tm-track-copy" }, il = { class: "tm-track-duration" }, al = {
	key: 1,
	class: "tm-empty compact"
}, ol = /* @__PURE__ */ ar({
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
		return (t, i) => (J(), Y("details", Yc, [X("summary", null, [X("span", null, [X("strong", null, U(e.item.track_list_label || "Current Track List"), 1), X("small", null, U(e.item.track_list?.length || 0) + " tracks", 1)]), X("span", Xc, [X("label", {
			class: "tm-shuffle",
			onClick: i[0] ||= os(() => {}, ["stop"])
		}, [X("input", {
			type: "checkbox",
			checked: !!e.item.track_list_shuffle,
			disabled: e.busy("shuffle"),
			onChange: r
		}, null, 40, Zc), i[1] ||= Z(" Shuffle ", -1)]), i[2] ||= X("span", {
			class: "tm-queue-chevron",
			"aria-hidden": "true"
		}, "⌄", -1)])]), e.item.track_list?.length ? (J(), Y("div", Qc, [(J(!0), Y(q, null, K(e.item.track_list, (t) => (J(), Y("button", {
			key: t.id || t.position,
			type: "button",
			class: z(["tm-track", {
				active: t.active,
				pending: e.busy(`track:${t.id}`)
			}]),
			disabled: e.busy(`track:${t.id}`),
			"aria-current": t.active ? "true" : void 0,
			title: `Double-click to play ${t.title || "this track"}`,
			onDblclick: (e) => n(t)
		}, [
			X("span", el, U(t.active ? "▶" : t.position), 1),
			t.image_src ? (J(), Y("img", {
				key: 0,
				class: "tm-track-art",
				src: t.image_src,
				alt: t.image_alt || "",
				loading: "lazy"
			}, null, 8, tl)) : (J(), Y("span", nl, "♫")),
			X("span", rl, [X("strong", null, U(t.title || "Untitled"), 1), X("small", null, U([t.artist, t.album].filter(Boolean).join(" · ") || "Unknown artist"), 1)]),
			X("span", il, U(t.duration || ""), 1)
		], 42, $c))), 128))])) : (J(), Y("div", al, "Play an album, artist, genre, or search to create a track list."))]));
	}
}), sl = /* @__PURE__ */ ar({
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
				let e = !!document.querySelector(".cerb-modal.active, .cerb-modal.closing, .tater-popup-effect-backdrop");
				document.body.classList.toggle("modal-open", e);
			});
		}
		return En(() => n.open, (e) => {
			e && document.body.classList.add("modal-open");
		}, { immediate: !0 }), Cr(i), (t, n) => (J(), ea(Vn, { to: "body" }, [aa(Xa, {
			name: "tater-popup",
			appear: "",
			onBeforeEnter: i,
			onAfterLeave: i
		}, {
			default: yn(() => [e.open ? (J(), Y("div", {
				key: 0,
				class: z(["tater-popup-effect-backdrop", e.backdropClass]),
				onClick: n[0] ||= os((e) => r("close"), ["self"])
			}, [
				n[1] ||= X("span", {
					class: "tater-popup-effect-field",
					"aria-hidden": "true"
				}, null, -1),
				n[2] ||= X("span", {
					class: "tater-popup-effect-burst",
					"aria-hidden": "true"
				}, null, -1),
				Ar(t.$slots, "default")
			], 2)) : Q("", !0)]),
			_: 3
		})]));
	}
}), cl = [
	"aria-label",
	"title",
	"aria-expanded"
], ll = {
	id: "tm-player-details",
	class: "tm-player-main"
}, ul = { class: "tm-art-wrap" }, dl = ["src", "alt"], fl = {
	key: 1,
	class: "tm-art tm-art-placeholder",
	"aria-hidden": "true"
}, pl = { class: "tm-now-playing" }, ml = [
	"max",
	"value",
	"disabled"
], hl = {
	class: "tm-progress-times",
	"aria-live": "off"
}, gl = {
	key: 0,
	class: "tm-badges"
}, _l = { class: "tm-player-controls" }, vl = {
	class: "tm-seek-controls",
	"aria-label": "Seek controls"
}, yl = ["disabled"], bl = ["disabled"], xl = {
	class: "tm-transport",
	"aria-label": "Playback controls"
}, Sl = [
	"disabled",
	"aria-label",
	"title",
	"onClick"
], Cl = { class: "tm-volume-speakers" }, wl = ["aria-label"], Tl = {
	key: 0,
	class: "tm-player-facts"
}, El = {
	class: "tm-modal",
	role: "dialog",
	"aria-modal": "true",
	"aria-labelledby": "tm-speaker-title"
}, Dl = { id: "tm-speaker-title" }, Ol = { class: "tm-modal-body" }, kl = ["disabled"], Al = /* @__PURE__ */ ar({
	__name: "MusicPlayer",
	props: {
		item: {},
		busy: { type: Function },
		run: { type: Function }
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ G(!1), r = /* @__PURE__ */ G(75), i = /* @__PURE__ */ G({}), a = /* @__PURE__ */ G(0), o = /* @__PURE__ */ G(!1), s = /* @__PURE__ */ G(!1), c = /* @__PURE__ */ G(!1), l = /* @__PURE__ */ G(!1), u, d = $(() => t.item.fields?.find((e) => e.key === "volume_percent")), f = $(() => t.item.popup_fields || []), p = $(() => t.item.playback || {}), m = $(() => Math.max(0, Number(p.value.duration_seconds || 0))), h = $(() => !!(p.value.seekable && m.value > 0));
		function g(e) {
			return Array.isArray(e) ? e.map((e) => e && typeof e == "object" ? { ...e } : e) : e && typeof e == "object" ? { ...e } : e;
		}
		En(d, (e) => {
			e && !l.value && (r.value = Number(e.value ?? 75));
		}, { immediate: !0 });
		function _() {
			let e = p.value, t = Math.max(0, Number(e.position_seconds || 0)), n = Number(e.position_updated_at || 0);
			return String(e.status || "").toLowerCase() === "playing" && n > 0 && (t += Math.max(0, Date.now() / 1e3 - n)), m.value > 0 ? Math.min(m.value, t) : t;
		}
		function v() {
			o.value || (a.value = _());
		}
		En(p, v, {
			immediate: !0,
			deep: !0
		}), br(() => {
			u = window.setInterval(v, 250);
		}), Cr(() => {
			u !== void 0 && window.clearInterval(u);
		}), En(f, (e) => {
			(!n.value || !c.value) && y(e);
		}, { immediate: !0 });
		function y(e = f.value) {
			i.value = Object.fromEntries(e.map((e) => [e.key, g(e.value)]));
		}
		function b() {
			y(), c.value = !1, n.value = !0;
		}
		function x() {
			n.value = !1, c.value = !1, y();
		}
		function S(e) {
			return e.endsWith("_play") ? "primary" : e.endsWith("_stop") ? "stop" : "";
		}
		function C(e, t) {
			return e.endsWith("_previous") ? "⏮" : e.endsWith("_play") ? "▶" : e.endsWith("_stop") ? "■" : e.endsWith("_next") ? "⏭" : t;
		}
		async function w(e) {
			await t.run(e, {
				id: t.item.id,
				values: { volume_percent: r.value }
			}, "transport");
		}
		async function T() {
			let e = d.value;
			if (!e?.action) {
				l.value = !1;
				return;
			}
			let n = await t.run(e.action, {
				id: t.item.id,
				values: { volume_percent: r.value }
			}, "volume");
			l.value = !1, n || (r.value = Number(d.value?.value ?? r.value));
		}
		function E(e) {
			r.value = Number(e), l.value = !0;
		}
		function D(e) {
			let t = Math.max(0, Math.round(Number(e) || 0)), n = Math.floor(t / 3600), r = Math.floor(t % 3600 / 60), i = t % 60;
			return n > 0 ? `${n}:${String(r).padStart(2, "0")}:${String(i).padStart(2, "0")}` : `${r}:${String(i).padStart(2, "0")}`;
		}
		function O(e) {
			o.value = !0, a.value = Number(e.target.value || 0);
		}
		async function k(e) {
			a.value = Number(e.target.value || 0);
			let n = p.value.seek_action;
			if (!n) {
				o.value = !1;
				return;
			}
			let r = await t.run(n, {
				id: t.item.id,
				values: { position_seconds: a.value }
			}, "seek");
			o.value = !1, r || v();
		}
		async function A(e) {
			if (!h.value) return;
			let n = e * Math.max(1, Number(p.value.seek_step_seconds || 15));
			a.value = Math.max(0, Math.min(m.value, a.value + n));
			let r = p.value.seek_relative_action;
			r && (await t.run(r, {
				id: t.item.id,
				values: { delta_seconds: n }
			}, "seek") || v());
		}
		async function ee() {
			t.item.save_action && await t.run(t.item.save_action, {
				id: t.item.id,
				values: i.value
			}, "speakers") && (c.value = !1, y(), n.value = !1);
		}
		function j(e, t) {
			i.value = {
				...i.value,
				[e.key]: t
			}, c.value = !0;
		}
		return (t, o) => (J(), Y("section", {
			class: z(["tm-player", { "is-collapsed": s.value }]),
			"aria-label": "Music player"
		}, [
			X("button", {
				type: "button",
				class: "tm-player-size-toggle",
				"aria-label": s.value ? "Expand music player" : "Switch to mini player",
				title: s.value ? "Expand music player" : "Switch to mini player",
				"aria-expanded": !s.value,
				"aria-controls": "tm-player-details",
				onClick: o[0] ||= (e) => s.value = !s.value
			}, [(J(), Y("svg", {
				class: z(["tm-player-size-icon", { "is-up": !s.value }]),
				viewBox: "0 0 16 16",
				"aria-hidden": "true"
			}, [...o[3] ||= [X("path", { d: "m3.5 6 4.5 4 4.5-4" }, null, -1)]], 2))], 8, cl),
			X("div", ll, [
				X("div", ul, [e.item.hero_image_src ? (J(), Y("img", {
					key: 0,
					class: "tm-art",
					src: e.item.hero_image_src,
					alt: e.item.hero_image_alt || ""
				}, null, 8, dl)) : (J(), Y("div", fl, "♫"))]),
				X("div", pl, [
					o[4] ||= X("div", { class: "tm-eyebrow" }, "Now playing", -1),
					X("h2", null, U(e.item.title || "Music Player"), 1),
					X("p", null, U(e.item.subtitle || e.item.detail), 1),
					X("div", { class: z(["tm-progress", { disabled: !h.value }]) }, [X("input", {
						type: "range",
						min: "0",
						max: m.value || 0,
						step: "1",
						value: a.value,
						disabled: !h.value || e.busy("seek"),
						"aria-label": "Track position",
						onInput: O,
						onChange: k
					}, null, 40, ml), X("div", hl, [X("span", null, U(D(a.value)), 1), X("span", null, U(D(m.value)), 1)])], 2),
					e.item.hero_badges?.length ? (J(), Y("div", gl, [(J(!0), Y(q, null, K(e.item.hero_badges, (e) => (J(), Y("span", {
						key: e.label,
						class: z(`tone-${e.tone || "muted"}`)
					}, U(e.label), 3))), 128))])) : Q("", !0)
				]),
				X("div", _l, [
					X("div", vl, [X("button", {
						type: "button",
						disabled: !h.value || e.busy("seek"),
						"aria-label": "Rewind 15 seconds",
						title: "Rewind 15 seconds",
						onClick: o[1] ||= (e) => A(-1)
					}, [...o[5] ||= [Z(" ↶ ", -1), X("span", null, "15", -1)]], 8, yl), X("button", {
						type: "button",
						disabled: !h.value || e.busy("seek"),
						"aria-label": "Forward 15 seconds",
						title: "Forward 15 seconds",
						onClick: o[2] ||= (e) => A(1)
					}, [...o[6] ||= [Z(" ↷ ", -1), X("span", null, "15", -1)]], 8, bl)]),
					X("div", xl, [(J(!0), Y(q, null, K(e.item.actions || [], (t) => (J(), Y("button", {
						key: t.action,
						type: "button",
						class: z(S(t.action)),
						disabled: e.busy("transport"),
						"aria-label": t.aria_label || t.label,
						title: t.tooltip || t.label,
						onClick: (e) => w(t.action)
					}, U(C(t.action, t.label || "Run")), 11, Sl))), 128))]),
					X("div", Cl, [d.value ? (J(), ea(jc, {
						key: 0,
						field: d.value,
						"model-value": r.value,
						compact: "",
						"onUpdate:modelValue": E,
						onChange: T
					}, null, 8, ["field", "model-value"])) : Q("", !0), X("button", {
						type: "button",
						class: "tm-speaker-button",
						"aria-label": e.item.settings_aria_label || "Choose speakers and players",
						title: "Choose speakers and players",
						onClick: b
					}, [...o[7] ||= [X("span", { "aria-hidden": "true" }, "🔊", -1), X("span", { class: "tm-speaker-label" }, "Players", -1)]], 8, wl)])
				])
			]),
			!s.value && e.item.summary_rows?.length ? (J(), Y("div", Tl, [(J(!0), Y(q, null, K(e.item.summary_rows, (e) => (J(), Y("div", { key: e.label }, [X("span", null, U(e.label), 1), X("strong", null, U(e.value || "—"), 1)]))), 128))])) : Q("", !0),
			s.value ? Q("", !0) : (J(), ea(ol, {
				key: 1,
				item: e.item,
				busy: e.busy,
				run: e.run
			}, null, 8, [
				"item",
				"busy",
				"run"
			])),
			aa(sl, {
				open: n.value,
				"backdrop-class": "tm-modal-backdrop",
				onClose: x
			}, {
				default: yn(() => [X("section", El, [
					X("header", null, [X("div", null, [o[8] ||= X("div", { class: "tm-eyebrow" }, "Playback destination", -1), X("h3", Dl, U(e.item.settings_title || "Choose Speakers & Players"), 1)]), X("button", {
						type: "button",
						class: "tm-close",
						"aria-label": "Close",
						onClick: x
					}, "×")]),
					X("div", Ol, [(J(!0), Y(q, null, K(f.value, (e) => (J(), ea(jc, {
						key: e.key,
						field: e,
						"model-value": i.value[e.key],
						"onUpdate:modelValue": (t) => j(e, t)
					}, null, 8, [
						"field",
						"model-value",
						"onUpdate:modelValue"
					]))), 128))]),
					X("footer", null, [X("button", {
						type: "button",
						class: "tm-button secondary",
						onClick: x
					}, "Cancel"), X("button", {
						type: "button",
						class: "tm-button primary",
						disabled: e.busy("speakers"),
						onClick: ee
					}, " Set players ", 8, kl)])
				])]),
				_: 1
			}, 8, ["open"])
		], 2));
	}
}), jl = {
	class: "tm-recommendations",
	"aria-label": "Tater Recommendations"
}, Ml = { class: "tm-recommendations-heading" }, Nl = { key: 0 }, Pl = ["disabled"], Fl = {
	key: 0,
	class: "tm-recommendation-grid"
}, Il = { class: "tm-recommendation-hero" }, Ll = ["src", "alt"], Rl = {
	key: 1,
	class: "tm-recommendation-placeholder",
	"aria-hidden": "true"
}, zl = {
	key: 2,
	class: "tm-badges"
}, Bl = { class: "tm-recommendation-copy" }, Vl = { class: "tm-recommendation-items" }, Hl = ["src", "alt"], Ul = {
	key: 1,
	class: "tm-recommendation-entry-art",
	"aria-hidden": "true"
}, Wl = { key: 0 }, Gl = ["disabled", "onClick"], Kl = {
	key: 1,
	class: "tm-empty tm-recommendations-empty"
}, ql = /* @__PURE__ */ ar({
	__name: "RecommendationsBrowser",
	props: {
		items: {},
		busy: { type: Function },
		run: { type: Function }
	},
	setup(e) {
		let t = e, n = $(() => t.items.find((e) => e.card_variant === "recommendations_intro")), r = $(() => t.items.filter((e) => e.card_variant === "recommendation_playlist"));
		async function i() {
			let e = n.value;
			!e?.run_action || !e.refresh_available || await t.run(e.run_action, {
				id: e.id,
				values: {}
			}, "recommendations:refresh");
		}
		async function a(e) {
			e.run_action && await t.run(e.run_action, {
				id: e.id,
				values: {}
			}, `recommendations:${e.id}`);
		}
		return (t, o) => (J(), Y("section", jl, [X("header", Ml, [X("div", null, [
			o[0] ||= X("div", { class: "tm-eyebrow" }, "Made for your ears", -1),
			X("h2", null, U(n.value?.title || "Tater Recommendations"), 1),
			X("p", null, U(n.value?.subtitle || "Named playlists shaped by what you listen to."), 1),
			n.value?.detail ? (J(), Y("small", Nl, U(n.value.detail), 1)) : Q("", !0)
		]), X("button", {
			type: "button",
			class: "tm-button primary",
			disabled: !n.value?.refresh_available || e.busy("recommendations:refresh") || n.value?.refresh_running,
			onClick: i
		}, U(e.busy("recommendations:refresh") || n.value?.refresh_running ? "Tater is mixing…" : n.value?.run_label || "Refresh Recommendations"), 9, Pl)]), r.value.length ? (J(), Y("div", Fl, [(J(!0), Y(q, null, K(r.value, (t) => (J(), Y("article", {
			key: t.id,
			class: "tm-recommendation-card"
		}, [
			X("div", Il, [t.hero_image_src ? (J(), Y("img", {
				key: 0,
				src: t.hero_image_src,
				alt: t.hero_image_alt || "",
				loading: "lazy"
			}, null, 8, Ll)) : (J(), Y("div", Rl, "♫")), t.hero_badges?.length ? (J(), Y("div", zl, [(J(!0), Y(q, null, K(t.hero_badges, (e) => (J(), Y("span", {
				key: e.label,
				class: z(`tone-${e.tone || "muted"}`)
			}, U(e.label), 3))), 128))])) : Q("", !0)]),
			X("div", Bl, [
				o[1] ||= X("div", { class: "tm-eyebrow" }, "Tater mix", -1),
				X("h3", null, U(t.title || "Tater Mix"), 1),
				X("p", null, U(t.subtitle), 1)
			]),
			X("div", Vl, [(J(!0), Y(q, null, K(t.recommendation_items || [], (e) => (J(), Y("div", {
				key: e.id,
				class: "tm-recommendation-entry"
			}, [e.image_src ? (J(), Y("img", {
				key: 0,
				src: e.image_src,
				alt: e.image_alt || "",
				loading: "lazy"
			}, null, 8, Hl)) : (J(), Y("span", Ul, "♫")), X("div", null, [
				X("small", null, U(e.type === "album" ? `Album · ${e.track_count || 0} tracks` : "Song"), 1),
				X("strong", null, U(e.title || "Untitled"), 1),
				X("span", null, U([e.artist, e.type === "song" ? e.album : ""].filter(Boolean).join(" · ")), 1),
				e.reason ? (J(), Y("p", Wl, U(e.reason), 1)) : Q("", !0)
			])]))), 128))]),
			X("footer", null, [X("button", {
				type: "button",
				class: "tm-button primary",
				disabled: e.busy(`recommendations:${t.id}`),
				onClick: (e) => a(t)
			}, U(e.busy(`recommendations:${t.id}`) ? "Starting…" : `▶ ${t.run_label || "Play Playlist"}`), 9, Gl), o[2] ||= X("small", null, "Plays on the destinations selected above.", -1)])
		]))), 128))])) : (J(), Y("div", Kl, [o[3] ||= X("strong", null, "No mixes yet", -1), X("span", null, U(n.value?.detail || "Play some music and Tater will start learning your taste."), 1)]))]));
	}
}), Jl = { class: "tm-settings-card" }, Yl = {
	key: 0,
	class: "tm-badges"
}, Xl = {
	key: 0,
	class: "tm-card-detail"
}, Zl = { key: 3 }, Ql = ["disabled", "onClick"], $l = ["disabled"], eu = /* @__PURE__ */ ar({
	__name: "SettingsCard",
	props: {
		item: {},
		busy: { type: Function },
		run: { type: Function }
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ Ct({}), r = /* @__PURE__ */ new Set(), i = /* @__PURE__ */ G(null), a = 0, o = null;
		function s() {
			a = 0;
			let e = i.value;
			if (!e) return;
			let t = window.getComputedStyle(e), n = Number.parseFloat(t.gridAutoRows) || 8, r = Number.parseFloat(t.rowGap) || 13, o = Array.from(e.children).filter((e) => e instanceof HTMLElement);
			o.forEach((e) => {
				e.style.gridRowEnd = "auto";
			}), o.forEach((e) => {
				let t = e.getBoundingClientRect().height, i = Math.max(1, Math.ceil((t + r) / (n + r)));
				e.style.gridRowEnd = `span ${i}`;
			});
		}
		function c() {
			window.cancelAnimationFrame(a), a = window.requestAnimationFrame(s);
		}
		function l() {
			o?.disconnect();
			let e = i.value;
			e && (o = new ResizeObserver(c), o.observe(e), Array.from(e.children).forEach((e) => o?.observe(e)), c());
		}
		function u(e) {
			return Array.isArray(e) ? e.map((e) => e && typeof e == "object" ? { ...e } : e) : e && typeof e == "object" ? { ...e } : e;
		}
		En(() => t.item.fields, (e) => {
			for (let t of e || []) r.has(t.key) || (n[t.key] = u(t.value));
			sn().then(l);
		}, {
			immediate: !0,
			deep: !0
		}), br(() => void sn().then(l)), Cr(() => {
			window.cancelAnimationFrame(a), o?.disconnect();
		});
		function d(e, t) {
			n[e.key] = t, r.add(e.key);
		}
		async function f() {
			t.item.save_action && await t.run(t.item.save_action, {
				id: t.item.id,
				values: { ...n }
			}, `item:${t.item.id}:save`) && r.clear();
		}
		async function p(e) {
			e.confirm && !window.confirm(e.confirm) || await t.run(e.action, {
				id: t.item.id,
				values: { ...n }
			}, `item:${t.item.id}:${e.action}`) && r.clear();
		}
		return (t, r) => (J(), Y("article", Jl, [
			X("header", null, [X("div", null, [X("h3", null, U(e.item.title || e.item.id), 1), X("p", null, U(e.item.subtitle), 1)]), e.item.hero_badges?.length ? (J(), Y("div", Yl, [(J(!0), Y(q, null, K(e.item.hero_badges, (e) => (J(), Y("span", {
				key: e.label,
				class: z(`tone-${e.tone || "muted"}`)
			}, U(e.label), 3))), 128))])) : Q("", !0)]),
			e.item.detail ? (J(), Y("p", Xl, U(e.item.detail), 1)) : Q("", !0),
			e.item.fields_dropdown && e.item.fields?.length ? (J(), Y("details", {
				key: 1,
				class: "tm-settings-fields",
				onToggle: c
			}, [r[0] ||= X("summary", null, "Connection settings", -1), X("div", {
				ref_key: "fieldGrid",
				ref: i,
				class: "tm-form-grid"
			}, [(J(!0), Y(q, null, K(e.item.fields, (e) => (J(), ea(jc, {
				key: e.key,
				field: e,
				"model-value": n[e.key],
				compact: !!e.compact,
				"onUpdate:modelValue": (t) => d(e, t)
			}, null, 8, [
				"field",
				"model-value",
				"compact",
				"onUpdate:modelValue"
			]))), 128))], 512)], 32)) : e.item.fields?.length ? (J(), Y("div", {
				key: 2,
				ref_key: "fieldGrid",
				ref: i,
				class: "tm-form-grid"
			}, [(J(!0), Y(q, null, K(e.item.fields, (e) => (J(), ea(jc, {
				key: e.key,
				field: e,
				"model-value": n[e.key],
				compact: !!e.compact,
				"onUpdate:modelValue": (t) => d(e, t)
			}, null, 8, [
				"field",
				"model-value",
				"compact",
				"onUpdate:modelValue"
			]))), 128))], 512)) : Q("", !0),
			e.item.actions?.length || e.item.save_action ? (J(), Y("footer", Zl, [(J(!0), Y(q, null, K(e.item.actions || [], (t) => (J(), Y("button", {
				key: t.action,
				type: "button",
				class: z(["tm-button", t.tone === "danger" ? "danger" : t.action.includes("activate") ? "primary" : "secondary"]),
				disabled: e.busy(`item:${e.item.id}:${t.action}`),
				onClick: (e) => p(t)
			}, U(t.label || "Run"), 11, Ql))), 128)), e.item.save_action ? (J(), Y("button", {
				key: 0,
				type: "button",
				class: "tm-button primary",
				disabled: e.busy(`item:${e.item.id}:save`),
				onClick: f
			}, U(e.item.save_label || "Save"), 9, $l)) : Q("", !0)])) : Q("", !0)
		]));
	}
}), tu = { class: "tater-music-core" }, nu = {
	key: 0,
	class: "tm-error"
}, ru = { class: "tm-page-heading" }, iu = ["title"], au = {
	key: 0,
	class: "tm-stats",
	"aria-label": "Music library status"
}, ou = {
	class: "tm-tabs",
	"aria-label": "Music Core sections"
}, su = ["onClick"], cu = {
	key: 0,
	class: "tm-empty"
}, lu = {
	key: 5,
	class: "tm-error-toast",
	role: "alert"
}, uu = /* @__PURE__ */ ar({
	__name: "MusicCoreApp",
	props: {
		state: {},
		options: {}
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ G(""), r = /* @__PURE__ */ G(/* @__PURE__ */ new Set()), i = /* @__PURE__ */ G(""), a = /* @__PURE__ */ G("connecting"), o = null, s = 0, c = $(() => t.state.payload || {}), l = $(() => c.value.ui || {}), u = $(() => l.value.item_forms || []), d = $(() => u.value.find((e) => e.group === "player")), f = $(() => l.value.manager_tabs || []), p = $(() => f.value.find((e) => e.key === n.value) || f.value[0]), m = $(() => {
			let e = p.value;
			return !e || e.source === "grouped_items" ? [] : u.value.filter((t) => !e.item_group || t.group === e.item_group);
		});
		En(f, (e) => {
			if (!e.some((e) => e.key === n.value)) {
				let t = String(l.value.default_tab || "");
				n.value = e.some((e) => e.key === t) ? t : e[0]?.key || "";
			}
		}, {
			immediate: !0,
			deep: !0
		});
		function h(e) {
			return r.value.has(e);
		}
		function g(e, t) {
			let n = new Set(r.value);
			t ? n.add(e) : n.delete(e), r.value = n;
		}
		async function _() {
			t.state.payload = await mc(t.options.tabEndpoint);
		}
		async function v(e, n, r = e) {
			if (!e || h(r)) return !1;
			i.value = "", g(r, !0);
			try {
				return await hc(t.options.actionEndpoint, e, n), await _(), !0;
			} catch (e) {
				return i.value = e instanceof Error ? e.message : String(e || "Music action failed."), !1;
			} finally {
				g(r, !1);
			}
		}
		function y() {
			o && o.close(), s && window.clearTimeout(s), a.value = "connecting", o = new EventSource(t.options.eventsEndpoint), o.addEventListener("core-tab", (e) => {
				try {
					t.state.payload = JSON.parse(e.data), a.value = "live";
				} catch {}
			}), o.addEventListener("open", () => {
				a.value = "live";
			}), o.addEventListener("error", () => {
				a.value = "offline", o?.close(), o = null, s = window.setTimeout(y, 3e3);
			});
		}
		return br(y), Cr(() => {
			s && window.clearTimeout(s), o?.close(), o = null;
		}), (e, t) => (J(), Y("main", tu, [c.value.error ? (J(), Y("div", nu, U(c.value.error), 1)) : (J(), Y(q, { key: 1 }, [
			X("header", ru, [X("div", null, [
				t[1] ||= X("div", { class: "tm-eyebrow" }, "Tater Music", -1),
				X("h1", null, U(l.value.title || "Music Core"), 1),
				X("p", null, U(c.value.summary), 1)
			]), X("div", {
				class: z(["tm-live-state", a.value]),
				title: `Music updates: ${a.value}`
			}, [t[2] ||= X("span", null, null, -1), Z(U(a.value === "live" ? "Live" : a.value === "connecting" ? "Connecting" : "Reconnecting"), 1)], 10, iu)]),
			c.value.stats?.length ? (J(), Y("section", au, [(J(!0), Y(q, null, K(c.value.stats, (e) => (J(), Y("div", { key: e.label }, [X("span", null, U(e.label), 1), X("strong", null, U(e.value ?? "—"), 1)]))), 128))])) : Q("", !0),
			d.value ? (J(), ea(Al, {
				key: 1,
				item: d.value,
				busy: h,
				run: v
			}, null, 8, ["item"])) : Q("", !0),
			X("nav", ou, [(J(!0), Y(q, null, K(f.value, (e) => (J(), Y("button", {
				key: e.key,
				type: "button",
				class: z({ active: n.value === e.key }),
				onClick: (t) => n.value = e.key
			}, U(e.label || e.key), 11, su))), 128))]),
			p.value?.source === "grouped_items" ? (J(), ea(Jc, {
				key: 2,
				groups: p.value.groups || [],
				items: u.value,
				busy: h,
				run: v
			}, null, 8, ["groups", "items"])) : p.value?.key === "recommendations" ? (J(), ea(ql, {
				key: 3,
				items: m.value,
				busy: h,
				run: v
			}, null, 8, ["items"])) : (J(), Y("section", {
				key: 4,
				class: z(["tm-settings-grid", `group-${p.value?.item_group || "all"}`])
			}, [(J(!0), Y(q, null, K(m.value, (e) => (J(), ea(eu, {
				key: e.id,
				item: e,
				busy: h,
				run: v
			}, null, 8, ["item"]))), 128)), m.value.length ? Q("", !0) : (J(), Y("div", cu, U(p.value?.empty_message || c.value.empty_message || "Nothing is available here yet."), 1))], 2)),
			i.value ? (J(), Y("div", lu, [X("span", null, U(i.value), 1), X("button", {
				type: "button",
				"aria-label": "Dismiss",
				onClick: t[0] ||= (e) => i.value = ""
			}, "×")])) : Q("", !0)
		], 64))]));
	}
}), du = ["value"], fu = {
	key: 1,
	class: "tvf-section"
}, pu = { key: 0 }, mu = {
	key: 2,
	class: "tvf-field full"
}, hu = { key: 0 }, gu = {
	key: 3,
	class: "tv-toggle tvf-toggle full"
}, _u = ["checked"], vu = { key: 0 }, yu = {
	key: 4,
	class: "tvf-field tvf-multiselect full"
}, bu = ["checked", "onChange"], xu = { key: 0 }, Su = {
	key: 5,
	class: "tvf-field"
}, Cu = ["value"], wu = ["value"], Tu = { key: 0 }, Eu = {
	key: 6,
	class: "tvf-field full"
}, Du = [
	"value",
	"placeholder",
	"rows"
], Ou = { key: 0 }, ku = {
	key: 7,
	class: "tvf-field full"
}, Au = ["accept"], ju = { key: 0 }, Mu = {
	key: 8,
	class: "tvf-field"
}, Nu = { class: "tvf-range" }, Pu = [
	"value",
	"min",
	"max",
	"step"
], Fu = { key: 0 }, Iu = [
	"type",
	"value",
	"min",
	"max",
	"step",
	"placeholder"
], Lu = { key: 0 }, Ru = /* @__PURE__ */ ar({
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
		let n = e, r = t, i = $(() => u(n.field.type || "text").toLowerCase()), a = $(() => u(n.field.label || n.field.key || "Setting")), o = $(() => String(n.modelValue ?? "")), s = $(() => new Set(d(n.modelValue))), c = $(() => (Array.isArray(n.field.show_when_all) ? n.field.show_when_all : n.field.show_when && typeof n.field.show_when == "object" ? [n.field.show_when] : []).every((e) => {
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
		})), l = $(() => ["API_AUTH_KEY", "AUTH_TOKEN"].includes(u(n.field.key).toUpperCase()));
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
		return (t, n) => c.value ? (J(), Y(q, { key: 0 }, [i.value === "hidden" ? (J(), Y("input", {
			key: 0,
			type: "hidden",
			value: o.value
		}, null, 8, du)) : i.value === "section" || i.value === "header" ? (J(), Y("section", fu, [X("h3", null, U(a.value), 1), e.field.description ? (J(), Y("p", pu, U(e.field.description), 1)) : Q("", !0)])) : i.value === "readonly" || i.value === "read_only" ? (J(), Y("label", mu, [
			X("span", null, U(a.value), 1),
			X("output", null, U(o.value), 1),
			e.field.description ? (J(), Y("small", hu, U(e.field.description), 1)) : Q("", !0)
		])) : i.value === "checkbox" ? (J(), Y("label", gu, [X("input", {
			class: "tv-checkbox",
			type: "checkbox",
			checked: !!e.modelValue,
			onChange: m
		}, null, 40, _u), X("span", null, [X("strong", null, U(a.value), 1), e.field.description ? (J(), Y("small", vu, U(e.field.description), 1)) : Q("", !0)])])) : i.value === "multiselect" ? (J(), Y("fieldset", yu, [
			X("legend", null, U(a.value), 1),
			X("div", null, [(J(!0), Y(q, null, K(e.field.options || [], (e) => (J(), Y("label", { key: f(e) }, [X("input", {
				class: "tv-checkbox",
				type: "checkbox",
				checked: s.value.has(f(e)),
				onChange: (t) => h(f(e), t.target.checked)
			}, null, 40, bu), X("span", null, U(p(e)), 1)]))), 128))]),
			e.field.description ? (J(), Y("small", xu, U(e.field.description), 1)) : Q("", !0)
		])) : i.value === "select" ? (J(), Y("label", Su, [
			X("span", null, U(a.value), 1),
			X("select", {
				value: o.value,
				onChange: m
			}, [(J(!0), Y(q, null, K(e.field.options || [], (e) => (J(), Y("option", {
				key: f(e),
				value: f(e)
			}, U(p(e)), 9, wu))), 128))], 40, Cu),
			e.field.description ? (J(), Y("small", Tu, U(e.field.description), 1)) : Q("", !0)
		])) : i.value === "textarea" || i.value === "multiline" ? (J(), Y("label", Eu, [
			X("span", null, U(a.value), 1),
			X("textarea", {
				value: o.value,
				placeholder: e.field.placeholder,
				rows: e.field.rows || 4,
				onInput: m
			}, null, 40, Du),
			e.field.description ? (J(), Y("small", Ou, U(e.field.description), 1)) : Q("", !0)
		])) : i.value === "file" ? (J(), Y("label", ku, [
			X("span", null, U(a.value), 1),
			X("input", {
				type: "file",
				accept: e.field.accept,
				onChange: _
			}, null, 40, Au),
			X("small", null, U(e.modelValue ? "A saved value is present. Choose a file to replace it." : "No file saved."), 1),
			e.field.description ? (J(), Y("small", ju, U(e.field.description), 1)) : Q("", !0)
		])) : i.value === "range" ? (J(), Y("label", Mu, [
			X("span", null, U(a.value), 1),
			X("div", Nu, [X("input", {
				type: "range",
				value: Number(e.modelValue ?? e.field.default ?? 0),
				min: e.field.min ?? 0,
				max: e.field.max ?? 100,
				step: e.field.step ?? 1,
				onInput: m
			}, null, 40, Pu), X("output", null, U(e.modelValue) + U(e.field.suffix || ""), 1)]),
			e.field.description ? (J(), Y("small", Fu, U(e.field.description), 1)) : Q("", !0)
		])) : (J(), Y("label", {
			key: 9,
			class: z(["tvf-field", { full: e.field.full_width }])
		}, [
			X("span", null, U(a.value), 1),
			X("div", { class: z({ "tvf-input-actions": l.value }) }, [X("input", {
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
			}, null, 40, Iu), l.value ? (J(), Y(q, { key: 0 }, [X("button", {
				class: "tv-button",
				type: "button",
				onClick: y
			}, "Copy"), X("button", {
				class: "tv-button",
				type: "button",
				onClick: v
			}, "Generate")], 64)) : Q("", !0)], 2),
			e.field.description ? (J(), Y("small", Lu, U(e.field.description), 1)) : Q("", !0)
		], 2))], 64)) : Q("", !0);
	}
}), zu = /* @__PURE__ */ ar({
	__name: "LegacyCorePanel",
	props: {
		payload: {},
		tab: {},
		render: { type: Function },
		clear: { type: Function }
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ G(null);
		async function r() {
			await sn(), n.value && t.render?.(n.value, t.payload || {}, t.tab);
		}
		return En(() => [t.payload, t.tab], r, {
			immediate: !0,
			deep: !1
		}), Cr(() => {
			n.value && t.clear?.(n.value);
		}), (e, t) => (J(), Y("div", {
			ref_key: "host",
			ref: n,
			class: "tcx-legacy-host"
		}, null, 512));
	}
}), Bu = { class: "tater-vue-surface tcx-cores" }, Vu = { class: "tv-page-heading" }, Hu = { class: "tv-heading-actions" }, Uu = { class: "tv-metrics" }, Wu = {
	key: 1,
	class: "tv-notice error"
}, Gu = {
	class: "tv-tabs tcx-top-tabs core-top-tabs",
	"aria-label": "Core panels"
}, Ku = ["data-core-tab", "onClick"], qu = {
	key: 0,
	class: "tcx-tab-dot",
	title: "Core is stopped"
}, Ju = { key: 0 }, Yu = ["data-core-tab-panel", "data-core-tab-loaded"], Xu = {
	key: 0,
	class: "tv-empty"
}, Zu = {
	key: 3,
	class: "core-top-tab-panel active tcx-manage",
	"data-core-tab-panel": "manage"
}, Qu = {
	class: "tv-mini-tabs tcx-manage-tabs",
	"aria-label": "Core management"
}, $u = ["onClick"], ed = { key: 0 }, td = {
	key: 0,
	class: "tcx-card-grid"
}, nd = { class: "tv-eyebrow" }, rd = { class: "tp-version" }, id = ["onClick"], ad = { key: 1 }, od = ["onClick"], sd = {
	key: 0,
	class: "tv-empty"
}, cd = {
	key: 1,
	class: "tcx-card-grid"
}, ld = { class: "tv-eyebrow" }, ud = { class: "tv-state" }, dd = ["onClick"], fd = {
	key: 0,
	class: "tv-empty"
}, pd = {
	key: 2,
	class: "tcx-manage-list"
}, md = { class: "tv-panel tcx-manage-toolbar" }, hd = ["disabled"], gd = { class: "ti-row-actions" }, _d = ["disabled", "onClick"], vd = ["onClick"], yd = { class: "ti-purge" }, bd = ["onUpdate:modelValue"], xd = ["onClick"], Sd = {
	key: 0,
	class: "tv-empty"
}, Cd = {
	key: 3,
	class: "tv-panel tcx-repos"
}, wd = { class: "ti-repo-row builtin" }, Td = ["onClick"], Ed = {
	key: 0,
	class: "tv-empty compact"
}, Dd = { class: "tcx-repo-form" }, Od = {
	class: "tv-modal tcx-settings-modal",
	role: "dialog",
	"aria-modal": "true"
}, kd = { class: "tvb-field-grid" }, Ad = /* @__PURE__ */ ar({
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
				label: "Maintenance"
			},
			{
				id: "repos",
				label: "Repositories"
			}
		], i = /* @__PURE__ */ G(String(n.options.initialTab || "manage")), a = /* @__PURE__ */ G("installed"), o = /* @__PURE__ */ G(""), s = /* @__PURE__ */ G(""), c = /* @__PURE__ */ G(""), l = /* @__PURE__ */ G({}), u = /* @__PURE__ */ G(""), d = /* @__PURE__ */ G(""), f = /* @__PURE__ */ G([]), p = /* @__PURE__ */ G(null), m = /* @__PURE__ */ G({}), h = /* @__PURE__ */ Ct({}), g = /* @__PURE__ */ G({}), _ = null, v = 0, y = $(() => n.state.payload?.runtime || {}), b = $(() => n.state.payload?.shop || {}), x = $(() => n.state.payload?.tabs || {}), S = $(() => Array.isArray(y.value.items) ? y.value.items : []), C = $(() => Array.isArray(b.value.installed) ? b.value.installed : []), w = $(() => Array.isArray(b.value.catalog) ? b.value.catalog : []), T = $(() => w.value.filter((e) => !e.installed).sort(ie)), E = $(() => C.value.filter((e) => e.update_available)), D = $(() => S.value.filter((e) => !!e.running).length), O = $(() => (Array.isArray(x.value.tabs) ? x.value.tabs : []).filter((e) => P(e.core_key)).map((e) => ({
			...e,
			core_key: P(e.core_key)
		}))), k = $(() => /* @__PURE__ */ new Set(["manage", ...O.value.map((e) => e.core_key)])), A = $(() => O.value.find((e) => e.core_key === i.value) || null), ee = $(() => h[i.value] || null), j = $(() => ee.value?.payload || {}), M = $(() => P(j.value?.ui?.appearance).toLowerCase() === "music_library"), te = $(() => new Map(S.value.map((e) => [F(e.key), e]))), N = $(() => {
			let e = /* @__PURE__ */ new Map();
			return C.value.forEach((t) => {
				let n = P(t.module_key || `${t.id}_core`);
				n && e.set(F(n), t), t.id && e.set(F(t.id), t);
			}), e;
		}), ne = $(() => {
			let e = /* @__PURE__ */ new Set(), t = S.value.map((t) => {
				let n = P(t.key), r = N.value.get(F(n)) || N.value.get(F(L(n))) || null;
				return r && e.add(F(r.id)), {
					key: n,
					runtime: t,
					shop: r
				};
			});
			return C.value.forEach((n) => {
				e.has(F(n.id)) || t.push({
					key: P(n.module_key || `${n.id}_core`),
					runtime: null,
					shop: n
				});
			}), t.sort((e, t) => ae(e).localeCompare(ae(t), void 0, {
				sensitivity: "base",
				numeric: !0
			}));
		}), re = $(() => {
			let e = A.value?.core_key;
			if (!e) return null;
			let t = encodeURIComponent(e);
			return {
				initialPayload: j.value,
				coreKey: e,
				tabEndpoint: `${n.options.endpoints.runtime}/${t}/tab`,
				actionEndpoint: `${n.options.endpoints.runtime}/${t}/tab-action`,
				eventsEndpoint: `${n.options.endpoints.runtime}/${t}/tab-events`
			};
		});
		function P(e) {
			return String(e ?? "").trim();
		}
		function F(e) {
			return P(e).toLowerCase();
		}
		function I(e) {
			return encodeURIComponent(P(e));
		}
		function L(e) {
			return P(e).replace(/_core$/i, "");
		}
		function ie(e, t) {
			return P(e.name || e.id).localeCompare(P(t.name || t.id), void 0, {
				sensitivity: "base",
				numeric: !0
			});
		}
		function ae(e) {
			return P(e.runtime?.label || e.shop?.name || L(e.key));
		}
		function R(e) {
			return P(e.shop?.description || "Local Core module.");
		}
		function B(e) {
			let t = P(e.module_key || `${e.id}_core`);
			return te.value.get(F(t)) || te.value.get(F(e.id)) || null;
		}
		function V(e) {
			return e ? e.running ? "Running" : e.desired_running ? "Pending start" : "Stopped" : "Unavailable";
		}
		function oe(e, t = "success") {
			s.value = e, c.value = t === "error" ? e : "", n.options.onToast?.(e, t);
		}
		function se() {
			f.value = Array.isArray(b.value.repos?.additional) ? b.value.repos.additional.map((e) => ({ ...e })) : [];
		}
		function H(e) {
			return h[e] || (h[e] = { payload: {} }), h[e];
		}
		async function ce(e = !1) {
			e || (o.value = "Refreshing Cores…"), c.value = "";
			try {
				let [e, t, r] = await Promise.all([
					gs(n.options.endpoints.runtime),
					gs(n.options.endpoints.shop),
					gs(n.options.endpoints.tabs)
				]);
				n.state.payload = {
					runtime: e,
					shop: t,
					tabs: r
				}, se(), k.value.has(i.value) ? i.value !== "manage" && await le(i.value, !0) : await pe("manage");
			} catch (e) {
				oe(e instanceof Error ? e.message : "Core refresh failed.", "error");
			} finally {
				e || (o.value = "");
			}
		}
		async function le(e, t = !1) {
			let r = P(e);
			if (!r || r === "manage") {
				await ce(t);
				return;
			}
			g.value = {
				...g.value,
				[r]: !0
			};
			try {
				let e = await gs(`${n.options.endpoints.runtime}/${I(r)}/tab`);
				H(r).payload = e || {}, r === i.value && !ue(e) && fe(r, e);
			} catch (e) {
				H(r).payload = { error: e instanceof Error ? e.message : "Core panel failed to load." };
			} finally {
				g.value = {
					...g.value,
					[r]: !1
				};
			}
		}
		function ue(e) {
			return P(e?.ui?.appearance).toLowerCase() === "music_library";
		}
		function de() {
			_?.close(), _ = null, v && window.clearTimeout(v), v = 0;
		}
		function fe(e, t) {
			de(), !(i.value !== e || ue(t) || !t?.ui?.live_updates) && (_ = new EventSource(`${n.options.endpoints.runtime}/${I(e)}/tab-events`), _.addEventListener("core-tab", (t) => {
				try {
					H(e).payload = JSON.parse(t.data);
				} catch {}
			}), _.addEventListener("error", () => {
				_?.close(), _ = null, i.value === e && (v = window.setTimeout(() => fe(e, H(e).payload), 3e3));
			}));
		}
		async function pe(e) {
			let t = k.value.has(e) ? e : "manage";
			if (i.value = t, n.options.onTabChange?.(t), de(), t !== "manage") {
				let e = H(t);
				Object.keys(e.payload).length ? ue(e.payload) || fe(t, e.payload) : await le(t);
			}
		}
		async function me(e, t) {
			let r = P(e.key);
			if (r) {
				o.value = `${t === "start" ? "Starting" : "Stopping"} ${r}…`;
				try {
					await _s(`${n.options.endpoints.runtime}/${I(r)}/${t}`), oe(`${P(e.label || r)} ${t === "start" ? "started" : "stopped"}.`), await ce(!0), n.options.onHealthRefresh?.();
				} catch (e) {
					oe(e instanceof Error ? e.message : `Core ${t} failed.`, "error");
				} finally {
					o.value = "";
				}
			}
		}
		async function W(e, t = "") {
			if (!(e === "remove" && !window.confirm(`Remove ${t}?${l.value[t] ? " Its saved data will also be deleted." : ""}`))) {
				o.value = `${e.replaceAll("-", " ")} ${t || "Cores"}…`;
				try {
					let r = t ? { id: t } : {};
					e === "remove" && (r.purge_redis = !!l.value[t]);
					let i = await _s(`${n.options.endpoints.shop}/${e}`, r), o = Array.isArray(i.updated) ? i.updated.length : 0, s = Array.isArray(i.failed) ? i.failed.length : 0;
					oe(P(i.message) || (e === "update-all" ? `Update-all completed. Updated ${o}, failed ${s}.` : "Core action completed."), s ? "error" : "success"), await ce(!0), e === "install" && (a.value = "installed"), n.options.onHealthRefresh?.();
				} catch (e) {
					oe(e instanceof Error ? e.message : "Core action failed.", "error");
				} finally {
					o.value = "";
				}
			}
		}
		function he(e) {
			let t = e.value ?? e.default ?? "", n = P(e.type).toLowerCase();
			return n === "checkbox" ? typeof t == "string" ? [
				"1",
				"true",
				"yes",
				"on",
				"enabled"
			].includes(t.toLowerCase()) : !!t : n === "number" || n === "range" ? t === "" ? "" : Number(t) : n === "multiselect" ? Array.isArray(t) ? [...t] : P(t).split(",").map((e) => e.trim()).filter(Boolean) : t;
		}
		function ge(e) {
			return (Array.isArray(e.show_when_all) ? e.show_when_all : e.show_when && typeof e.show_when == "object" ? [e.show_when] : []).every((e) => {
				let t = P(e.source_key ?? e.key);
				if (!t) return !0;
				let n = [
					...e.any_of || [],
					...e.values || [],
					...e.equals === void 0 ? [] : [e.equals],
					...e.value === void 0 ? [] : [e.value]
				].map((e) => String(e ?? "").trim()), r = typeof m.value[t] == "boolean" ? m.value[t] ? "true" : "false" : String(m.value[t] ?? "").trim();
				return !n.length || n.includes(r);
			});
		}
		function _e(e) {
			p.value = e, m.value = Object.fromEntries((Array.isArray(e.settings) ? e.settings : []).filter((e) => P(e.key)).map((e) => [P(e.key), he(e)]));
		}
		async function ve() {
			let e = p.value;
			if (!e) return;
			let t = P(e.key);
			o.value = `Saving ${P(e.label || t)}…`;
			try {
				let r = Object.fromEntries((e.settings || []).filter((e) => P(e.key) && ![
					"section",
					"header",
					"readonly",
					"read_only",
					"led_preview"
				].includes(P(e.type).toLowerCase()) && ge(e)).map((e) => [P(e.key), m.value[P(e.key)]]));
				await _s(`${n.options.endpoints.runtime}/${I(t)}/settings`, { values: r }), oe(`Saved settings for ${P(e.label || t)}.`), p.value = null, await ce(!0);
			} catch (e) {
				oe(e instanceof Error ? e.message : "Core settings save failed.", "error");
			} finally {
				o.value = "";
			}
		}
		function ye() {
			let e = d.value.trim();
			if (!e) {
				oe("Repository URL is required.", "error");
				return;
			}
			if (f.value.some((t) => P(t.url).toLowerCase() === e.toLowerCase())) {
				oe("That repository is already added.", "error");
				return;
			}
			f.value.push({
				name: u.value.trim(),
				url: e
			}), u.value = "", d.value = "", s.value = "Repository added. Save repositories to apply it.", c.value = "";
		}
		async function be() {
			o.value = "Saving Core repositories…";
			try {
				await _s(`${n.options.endpoints.shop}/repos`, { repos: f.value }), oe("Core repositories saved."), await ce(!0);
			} catch (e) {
				oe(e instanceof Error ? e.message : "Repository save failed.", "error");
			} finally {
				o.value = "";
			}
		}
		function xe(e) {
			e.key === "Escape" && (p.value = null);
		}
		return En(() => n.state.payload, se, { deep: !1 }), En(O, (e) => {
			(/* @__PURE__ */ new Set(["manage", ...e.map((e) => e.core_key)])).has(i.value) || pe("manage");
		}, { immediate: !0 }), se(), window.addEventListener("keydown", xe), Cr(() => {
			de(), window.removeEventListener("keydown", xe);
		}), sn(() => void pe(i.value)), t({
			refresh: () => ce(!1),
			refreshTab: (e) => le(e, !0)
		}), (t, n) => (J(), Y(q, null, [X("div", Bu, [
			X("header", Vu, [n[8] ||= X("div", null, [
				X("span", { class: "tv-eyebrow" }, "System capabilities"),
				X("h1", null, "Cores"),
				X("p", null, "Run, configure, browse, and update Tater’s capability modules from one live workspace.")
			], -1), X("div", Hu, [X("span", { class: z(["tv-live-pill", { busy: !!o.value }]) }, [n[7] ||= X("i", null, null, -1), Z(U(o.value || "Live"), 1)], 2), X("button", {
				class: "tv-button",
				type: "button",
				onClick: n[0] ||= (e) => ce()
			}, "Refresh")])]),
			X("div", Uu, [
				X("div", null, [n[9] ||= X("span", null, "Installed", -1), X("strong", null, U(C.value.length || S.value.length), 1)]),
				X("div", null, [n[10] ||= X("span", null, "Running", -1), X("strong", null, U(D.value), 1)]),
				X("div", null, [n[11] ||= X("span", null, "Panels", -1), X("strong", null, U(O.value.length), 1)]),
				X("div", null, [n[12] ||= X("span", null, "Updates", -1), X("strong", null, U(Number(b.value.updates_available || E.value.length)), 1)])
			]),
			s.value || c.value ? (J(), Y("div", {
				key: 0,
				class: z(["tv-notice", { error: !!c.value }])
			}, U(c.value || s.value), 3)) : Q("", !0),
			b.value.errors?.length ? (J(), Y("div", Wu, U(b.value.errors.join(" • ")), 1)) : Q("", !0),
			X("nav", Gu, [(J(!0), Y(q, null, K(O.value, (e) => (J(), Y("button", {
				key: e.core_key,
				type: "button",
				class: z(["core-top-tab-btn", { active: i.value === e.core_key }]),
				"data-core-tab": e.core_key,
				onClick: (t) => pe(e.core_key)
			}, [Z(U(e.label || e.core_key), 1), e.requires_running && !e.running ? (J(), Y("span", qu)) : Q("", !0)], 10, Ku))), 128)), X("button", {
				type: "button",
				class: z(["core-top-tab-btn", { active: i.value === "manage" }]),
				"data-core-tab": "manage",
				onClick: n[1] ||= (e) => pe("manage")
			}, [Z(U(x.value.manage_label || "Manage"), 1), E.value.length ? (J(), Y("span", Ju, U(E.value.length), 1)) : Q("", !0)], 2)]),
			i.value === "manage" ? (J(), Y("section", Zu, [X("nav", Qu, [(J(), Y(q, null, K(r, (e) => X("button", {
				key: e.id,
				type: "button",
				class: z({ active: a.value === e.id }),
				onClick: (t) => a.value = e.id
			}, [Z(U(e.label), 1), e.id === "manage" && E.value.length ? (J(), Y("span", ed, U(E.value.length), 1)) : Q("", !0)], 10, $u)), 64))]), a.value === "installed" ? (J(), Y("div", td, [(J(!0), Y(q, null, K(ne.value, (e) => (J(), Y("article", {
				key: e.key,
				class: "tv-panel tcx-core-card"
			}, [
				X("header", null, [X("div", null, [X("span", nd, U(e.key), 1), X("h2", null, U(ae(e)), 1)]), X("span", { class: z(["tv-state", {
					good: e.runtime?.running,
					pending: e.runtime?.desired_running && !e.runtime?.running
				}]) }, U(V(e.runtime)), 3)]),
				X("p", null, U(R(e)), 1),
				X("div", rd, [
					X("span", null, "Installed " + U(e.shop?.installed_ver || "0.0.0"), 1),
					X("span", null, "Store " + U(e.shop?.store_ver || "-"), 1),
					X("span", null, U(e.shop?.source_label || "local"), 1)
				]),
				X("footer", null, [e.runtime?.settings?.length ? (J(), Y("button", {
					key: 0,
					class: "tv-button",
					type: "button",
					onClick: (t) => _e(e.runtime)
				}, "Settings", 8, id)) : (J(), Y("span", ad, U(e.runtime ? "No configurable settings" : "Runtime unavailable"), 1)), e.runtime ? (J(), Y("button", {
					key: 2,
					class: z(["tv-button", { primary: !e.runtime.running }]),
					type: "button",
					onClick: (t) => me(e.runtime, e.runtime.running ? "stop" : "start")
				}, U(e.runtime.running ? "Stop" : "Start"), 11, od)) : Q("", !0)])
			]))), 128)), ne.value.length ? Q("", !0) : (J(), Y("div", sd, "No installed Cores found."))])) : a.value === "store" ? (J(), Y("div", cd, [(J(!0), Y(q, null, K(T.value, (e) => (J(), Y("article", {
				key: e.id,
				class: "tv-panel tcx-core-card"
			}, [
				X("header", null, [X("div", null, [X("span", ld, U(e.id), 1), X("h2", null, U(e.name || e.id), 1)]), X("span", ud, "v" + U(e.version || "-"), 1)]),
				X("p", null, U(e.description || "No description provided."), 1),
				X("footer", null, [X("span", null, U(e.source_label || "Tater Shop"), 1), X("button", {
					class: "tv-button primary",
					type: "button",
					onClick: (t) => W("install", e.id)
				}, "Install", 8, dd)])
			]))), 128)), T.value.length ? Q("", !0) : (J(), Y("div", fd, "No additional Cores are available from the configured repositories."))])) : a.value === "manage" ? (J(), Y("div", pd, [
				X("div", md, [X("div", null, [
					n[13] ||= X("span", { class: "tv-eyebrow" }, "Maintenance", -1),
					n[14] ||= X("h2", null, "Manage installed Cores", -1),
					X("p", null, U(E.value.length) + " update" + U(E.value.length === 1 ? "" : "s") + " available. Running Cores restart automatically after an update.", 1)
				]), X("button", {
					class: "tv-button primary",
					type: "button",
					disabled: !E.value.length,
					onClick: n[2] ||= (e) => W("update-all")
				}, "Update all", 8, hd)]),
				(J(!0), Y(q, null, K(C.value.slice().sort(ie), (e) => (J(), Y("article", {
					key: e.id,
					class: "tv-panel tcx-manage-row"
				}, [X("div", null, [X("strong", null, U(e.name || e.id), 1), X("span", null, U(e.installed_ver || "0.0.0") + " → " + U(e.store_ver || "-") + " · " + U(V(B(e))), 1)]), X("div", gd, [
					X("button", {
						class: "tv-button",
						type: "button",
						disabled: !e.update_available,
						onClick: (t) => W("update", e.id)
					}, U(e.update_available ? "Update" : "Current"), 9, _d),
					B(e) ? (J(), Y("button", {
						key: 0,
						class: "tv-button",
						type: "button",
						onClick: (t) => me(B(e), B(e)?.running ? "stop" : "start")
					}, U(B(e)?.running ? "Stop" : "Start"), 9, vd)) : Q("", !0),
					X("label", yd, [bn(X("input", {
						"onUpdate:modelValue": (t) => l.value[e.id] = t,
						type: "checkbox"
					}, null, 8, bd), [[Jo, l.value[e.id]]]), n[15] ||= Z(" Delete data", -1)]),
					X("button", {
						class: "tv-button danger",
						type: "button",
						onClick: (t) => W("remove", e.id)
					}, "Remove", 8, xd)
				])]))), 128)),
				C.value.length ? Q("", !0) : (J(), Y("div", Sd, "No installed Cores found."))
			])) : (J(), Y("div", Cd, [
				n[19] ||= X("header", null, [X("div", null, [
					X("span", { class: "tv-eyebrow" }, "Trusted sources"),
					X("h2", null, "Core repositories"),
					X("p", null, "The built-in Core repository stays available. Add other trusted manifests below.")
				])], -1),
				X("article", wd, [X("div", null, [X("strong", null, U(b.value.repos?.default?.name || "Default"), 1), X("code", null, U(b.value.repos?.default?.url || "(not set)"), 1)]), n[16] ||= X("span", null, "Built-in", -1)]),
				(J(!0), Y(q, null, K(f.value, (e, t) => (J(), Y("article", {
					key: `${e.url}-${t}`,
					class: "ti-repo-row"
				}, [X("div", null, [X("strong", null, U(e.name || "Additional repository"), 1), X("code", null, U(e.url), 1)]), X("button", {
					class: "tv-button",
					type: "button",
					onClick: (e) => f.value.splice(t, 1)
				}, "Remove", 8, Td)]))), 128)),
				f.value.length ? Q("", !0) : (J(), Y("div", Ed, "No additional repositories configured.")),
				X("div", Dd, [
					X("label", null, [n[17] ||= X("span", null, "Name (optional)", -1), bn(X("input", {
						"onUpdate:modelValue": n[3] ||= (e) => u.value = e,
						type: "text",
						placeholder: "My Core Repo"
					}, null, 512), [[qo, u.value]])]),
					X("label", null, [n[18] ||= X("span", null, "Repository URL", -1), bn(X("input", {
						"onUpdate:modelValue": n[4] ||= (e) => d.value = e,
						type: "url",
						placeholder: "https://example.com/cores.json",
						onKeyup: cs(ye, ["enter"])
					}, null, 544), [[qo, d.value]])]),
					X("button", {
						class: "tv-button",
						type: "button",
						onClick: ye
					}, "Add"),
					X("button", {
						class: "tv-button primary",
						type: "button",
						onClick: be
					}, "Save repositories")
				])
			]))])) : (J(), Y("section", {
				key: 2,
				class: "core-top-tab-panel active tcx-core-panel",
				"data-core-tab-panel": i.value,
				"data-core-tab-loaded": g.value[i.value] ? "loading" : "1"
			}, [g.value[i.value] && !Object.keys(j.value).length ? (J(), Y("div", Xu, "Loading " + U(A.value?.label || i.value) + "…", 1)) : A.value && ee.value && M.value && re.value ? (J(), ea(uu, {
				key: 1,
				state: ee.value,
				options: re.value
			}, null, 8, ["state", "options"])) : A.value && ee.value ? (J(), ea(zu, {
				key: 2,
				payload: j.value,
				tab: A.value,
				render: e.options.renderCorePanel,
				clear: e.options.clearCorePanel
			}, null, 8, [
				"payload",
				"tab",
				"render",
				"clear"
			])) : Q("", !0)], 8, Yu))
		]), aa(sl, {
			open: !!p.value,
			onClose: n[6] ||= (e) => p.value = null
		}, {
			default: yn(() => [X("section", Od, [
				X("header", null, [X("div", null, [
					n[20] ||= X("span", { class: "tv-eyebrow" }, "Core settings", -1),
					X("h2", null, U(p.value?.label || p.value?.key), 1),
					n[21] ||= X("p", null, "Changes are applied to this Core’s runtime configuration.", -1)
				]), X("button", {
					class: "tv-button",
					type: "button",
					onClick: n[5] ||= (e) => p.value = null
				}, "Close")]),
				X("div", kd, [(J(!0), Y(q, null, K(p.value?.settings || [], (e) => (J(), ea(Ru, {
					key: e.key || e.label,
					field: e,
					"model-value": m.value[e.key],
					"all-values": m.value,
					"onUpdate:modelValue": (t) => m.value[e.key] = t
				}, null, 8, [
					"field",
					"model-value",
					"all-values",
					"onUpdate:modelValue"
				]))), 128))]),
				X("footer", null, [X("span", null, U(o.value || "Ready"), 1), X("button", {
					class: "tv-button primary",
					type: "button",
					onClick: ve
				}, "Save settings")])
			])]),
			_: 1
		}, 8, ["open"])], 64));
	}
}), jd = { class: "tater-vue-surface td-dashboard" }, Md = { class: "tv-page-heading" }, Nd = { key: 0 }, Pd = { key: 1 }, Fd = { class: "tv-heading-actions" }, Id = {
	key: 0,
	class: "tv-notice error"
}, Ld = {
	key: 1,
	class: "td-overview tv-panel"
}, Rd = {
	key: 2,
	class: "tv-panel td-updates"
}, zd = { class: "tv-panel-head" }, Bd = { class: "td-update-grid" }, Vd = ["onClick"], Hd = { key: 0 }, Ud = { key: 1 }, Wd = {
	key: 3,
	class: "tv-panel td-system-brief"
}, Gd = { class: "tv-panel-head" }, Kd = { class: "tv-eyebrow" }, qd = { key: 0 }, Jd = {
	key: 0,
	class: "td-brief"
}, Yd = {
	key: 1,
	class: "tv-metrics"
}, Xd = {
	key: 2,
	class: "td-items"
}, Zd = ["src", "alt"], Qd = {
	class: "tv-modal td-dashboard-controls",
	role: "dialog",
	"aria-modal": "true",
	"aria-label": "Dashboard controls"
}, $d = { class: "tv-control-list" }, ef = { class: "tv-toggle" }, tf = { class: "tv-toggle" }, nf = ["value"], rf = ["value"], af = ["value"], of = /* @__PURE__ */ ar({
	__name: "DashboardApp",
	props: {
		state: {},
		options: {}
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ G(!1), r = /* @__PURE__ */ G(""), i = /* @__PURE__ */ G(""), a = /* @__PURE__ */ G(t.options.initialPreferences?.showMetrics !== !1), o = /* @__PURE__ */ G(t.options.initialPreferences?.showMedia !== !1), s = 0, c = 0, l = null, u = /* @__PURE__ */ G(null), d = $(() => t.state.payload || {}), f = $(() => Array.isArray(d.value.sections) ? d.value.sections : []), p = $(() => [...f.value].sort((e, t) => T(e?.title || e?.id).localeCompare(T(t?.title || t?.id), void 0, { sensitivity: "base" }))), m = $(() => Array.isArray(d.value.briefs) ? d.value.briefs : []), h = $(() => new Map(m.value.map((e) => [String(e?.id || ""), e]))), g = $(() => d.value.updates && typeof d.value.updates == "object" ? d.value.updates : {}), _ = $(() => Array.isArray(g.value.groups) ? g.value.groups : []), v = $(() => d.value.settings?.personal || {}), y = $(() => d.value.settings?.refresh || {}), b = /* @__PURE__ */ G(""), x = /* @__PURE__ */ G(300), S = /* @__PURE__ */ G(3600), C = $(() => h.value.get("overview")), w = $(() => h.value.get("system"));
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
		function ee(e, n = "success") {
			t.options.onToast?.(e, n);
		}
		function j(e) {
			t.state.payload = e, t.options.onPayloadChange?.(e);
		}
		async function M(e = {}) {
			if (!(r.value && e.quiet)) {
				e.quiet || (r.value = e.snapshot ? "Refreshing live snapshot…" : "Refreshing dashboard…"), i.value = "";
				try {
					let n = t.options.dashboardEndpoint.includes("?") ? "&" : "?";
					j(await gs(e.snapshot ? `${t.options.dashboardEndpoint}${n}refresh_snapshot=true` : t.options.dashboardEndpoint)), e.quiet || ee("Dashboard refreshed.");
				} catch (t) {
					i.value = t instanceof Error ? t.message : "Dashboard refresh failed.", e.quiet || ee(i.value, "error");
				} finally {
					e.quiet || (r.value = "");
				}
			}
		}
		async function te() {
			r.value = "Generating fresh briefs…", i.value = "";
			try {
				j(await _s(t.options.refreshBriefsEndpoint, { brief_id: null })), ee("Dashboard brief refresh queued.");
			} catch (e) {
				i.value = e instanceof Error ? e.message : "Brief refresh failed.", ee(i.value, "error");
			} finally {
				r.value = "";
			}
		}
		async function N(e, n) {
			r.value = "Saving dashboard controls…", i.value = "";
			try {
				j(await _s(t.options.settingsEndpoint, e)), ee(n);
			} catch (e) {
				i.value = e instanceof Error ? e.message : "Dashboard settings failed to save.", ee(i.value, "error");
			} finally {
				r.value = "";
			}
		}
		function ne() {
			t.options.onPreferencesChange?.({
				showMetrics: a.value,
				showMedia: o.value
			});
		}
		function re() {
			window.clearInterval(s);
			let e = Number(y.value.refresh_interval_seconds || x.value || 0);
			e > 0 && (s = window.setInterval(() => void M({ quiet: !0 }), Math.max(15, e) * 1e3));
		}
		function P() {
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
		function F() {
			window.cancelAnimationFrame(c), c = window.requestAnimationFrame(P);
		}
		function I() {
			l?.disconnect();
			let e = u.value;
			e && (l = new ResizeObserver(F), l.observe(e), Array.from(e.children).forEach((e) => l?.observe(e)), F());
		}
		En([a, o], ne), En(() => t.state.payload, () => {
			b.value = T(v.value.person_id), x.value = Number(y.value.refresh_interval_seconds ?? 300), S.value = Number(y.value.brief_refresh_interval_seconds ?? 3600), re(), sn().then(I);
		}, { immediate: !0 }), br(() => {
			M({ quiet: !0 }), sn().then(I);
		}), Cr(() => {
			window.clearInterval(s), window.cancelAnimationFrame(c), l?.disconnect();
		});
		let L = [
			[0, "Off"],
			[30, "30 seconds"],
			[60, "1 minute"],
			[300, "5 minutes"],
			[900, "15 minutes"],
			[1800, "30 minutes"],
			[3600, "1 hour"],
			[7200, "2 hours"],
			[14400, "4 hours"]
		], ie = [
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
		return (e, t) => (J(), Y("div", jd, [
			X("header", Md, [X("div", null, [
				t[12] ||= X("span", { class: "tv-eyebrow" }, "Home at a glance", -1),
				t[13] ||= X("h1", null, "Dashboard", -1),
				X("p", null, [d.value.generated_at ? (J(), Y("span", Nd, "Updated " + U(E(d.value.generated_at)), 1)) : (J(), Y("span", Pd, "Live status, signals, and Tater summaries."))])
			]), X("div", Fd, [X("span", { class: z(["tv-live-pill", { busy: !!r.value }]) }, [t[14] ||= X("i", null, null, -1), Z(U(r.value || "Live"), 1)], 2), X("button", {
				type: "button",
				class: "tv-button",
				onClick: t[0] ||= (e) => n.value = !0
			}, "Controls")])]),
			i.value ? (J(), Y("div", Id, U(i.value), 1)) : Q("", !0),
			C.value?.text ? (J(), Y("section", Ld, [X("div", null, [t[15] ||= X("span", { class: "tv-eyebrow" }, "Today", -1), X("h2", null, U(C.value.title || "Home Brief"), 1)]), X("p", null, U(C.value.text), 1)])) : Q("", !0),
			_.value.length ? (J(), Y("section", Rd, [X("div", zd, [X("div", null, [t[16] ||= X("span", { class: "tv-eyebrow" }, "Update watch", -1), X("h2", null, U(Number(g.value.total || 0) ? `${g.value.total} available` : "Everything current"), 1)]), X("span", null, U(g.value.summary || "Firmware and Tater Shop surfaces checked."), 1)]), X("div", Bd, [(J(!0), Y(q, null, K(_.value, (e) => (J(), Y("button", {
				key: T(e.kind),
				type: "button",
				onClick: (t) => A(e.kind)
			}, [
				X("span", null, U(e.label || e.kind), 1),
				X("strong", null, U(e.error ? "Needs check" : Number(e.count || 0) ? `${e.count} available` : "Current"), 1),
				e.items?.length ? (J(), Y("small", Hd, U(e.items.slice(0, 3).map((e) => e.name || e.id).join(" • ")), 1)) : (J(), Y("small", Ud, U(e.error || "No pending updates"), 1))
			], 8, Vd))), 128))])])) : Q("", !0),
			w.value?.text ? (J(), Y("section", Wd, [
				t[17] ||= X("span", { class: "tv-eyebrow" }, "Tater", -1),
				X("h2", null, U(w.value.title || "System summary"), 1),
				X("p", null, U(w.value.text), 1)
			])) : Q("", !0),
			X("section", {
				ref_key: "sectionGrid",
				ref: u,
				class: "td-section-grid"
			}, [(J(!0), Y(q, null, K(p.value, (e) => (J(), Y("article", {
				key: T(e.id),
				class: z(["tv-panel td-section", `section-${T(e.id)}`])
			}, [
				X("header", Gd, [X("div", null, [
					X("span", Kd, U(e.id), 1),
					X("h2", null, U(e.title || e.id), 1),
					X("p", null, U(e.subtitle), 1)
				]), D(e)?.updated_at ? (J(), Y("span", qd, U(E(D(e)?.updated_at)), 1)) : Q("", !0)]),
				D(e)?.text ? (J(), Y("p", Jd, U(D(e)?.text), 1)) : Q("", !0),
				a.value && e.stats?.length ? (J(), Y("div", Yd, [(J(!0), Y(q, null, K(e.stats, (e) => (J(), Y("div", { key: T(e.label) }, [X("span", null, U(e.label), 1), X("strong", null, U(e.value ?? "-"), 1)]))), 128))])) : Q("", !0),
				O(e).length ? (J(), Y("div", Xd, [(J(!0), Y(q, null, K(O(e), (e, t) => (J(), Y("article", {
					key: T(e.id || e.title || t),
					class: "td-item"
				}, [o.value && k(e) ? (J(), Y("img", {
					key: 0,
					src: k(e),
					alt: T(e.image_alt || e.title || "Dashboard image"),
					loading: "lazy"
				}, null, 8, Zd)) : Q("", !0), X("div", null, [X("strong", null, U(e.title || e.name || e.label || "Signal"), 1), X("span", null, U(e.subtitle || e.when || e.state || e.detail), 1)])]))), 128))])) : Q("", !0)
			], 2))), 128))], 512),
			aa(sl, {
				open: n.value,
				onClose: t[11] ||= (e) => n.value = !1
			}, {
				default: yn(() => [X("section", Qd, [
					X("header", null, [t[18] ||= X("div", null, [X("span", { class: "tv-eyebrow" }, "Dashboard"), X("h2", null, "Controls")], -1), X("button", {
						class: "tv-button",
						type: "button",
						onClick: t[1] ||= (e) => n.value = !1
					}, "Close")]),
					X("div", $d, [
						X("label", ef, [bn(X("input", {
							"onUpdate:modelValue": t[2] ||= (e) => a.value = e,
							class: "tv-checkbox",
							type: "checkbox"
						}, null, 512), [[Jo, a.value]]), t[19] ||= X("span", null, [X("strong", null, "Metric pills"), X("small", null, "Show compact live readings inside each area.")], -1)]),
						X("label", tf, [bn(X("input", {
							"onUpdate:modelValue": t[3] ||= (e) => o.value = e,
							class: "tv-checkbox",
							type: "checkbox"
						}, null, 512), [[Jo, o.value]]), t[20] ||= X("span", null, [X("strong", null, "Media"), X("small", null, "Show snapshots and satellite images when available.")], -1)]),
						X("label", null, [t[21] ||= X("span", null, "Dashboard refresh", -1), bn(X("select", {
							"onUpdate:modelValue": t[4] ||= (e) => x.value = e,
							onChange: t[5] ||= (e) => N({
								refresh_interval_seconds: x.value,
								brief_refresh_interval_seconds: S.value
							}, "Dashboard refresh updated.")
						}, [(J(), Y(q, null, K(L, (e) => X("option", {
							key: e[0],
							value: e[0]
						}, U(e[1]), 9, nf)), 64))], 544), [[
							Zo,
							x.value,
							void 0,
							{ number: !0 }
						]])]),
						X("label", null, [t[22] ||= X("span", null, "Brief refresh", -1), bn(X("select", {
							"onUpdate:modelValue": t[6] ||= (e) => S.value = e,
							onChange: t[7] ||= (e) => N({
								refresh_interval_seconds: x.value,
								brief_refresh_interval_seconds: S.value
							}, "Brief refresh updated.")
						}, [(J(), Y(q, null, K(ie, (e) => X("option", {
							key: e[0],
							value: e[0]
						}, U(e[1]), 9, rf)), 64))], 544), [[
							Zo,
							S.value,
							void 0,
							{ number: !0 }
						]])]),
						X("label", null, [t[24] ||= X("span", null, "Personal profile", -1), bn(X("select", {
							"onUpdate:modelValue": t[8] ||= (e) => b.value = e,
							onChange: t[9] ||= (e) => N({ personal_person_id: b.value || null }, "Personal dashboard profile updated.")
						}, [t[23] ||= X("option", { value: "" }, "All people", -1), (J(!0), Y(q, null, K(v.value.people_options || [], (e) => (J(), Y("option", {
							key: T(e.value),
							value: T(e.value)
						}, U(e.label || e.value), 9, af))), 128))], 544), [[Zo, b.value]])])
					]),
					X("footer", null, [X("span", null, U(r.value || i.value), 1), X("div", null, [X("button", {
						class: "tv-button",
						type: "button",
						onClick: t[10] ||= (e) => M({ snapshot: !0 })
					}, "Refresh snapshot"), X("button", {
						class: "tv-button primary",
						type: "button",
						onClick: te
					}, "Generate briefs")])])
				])]),
				_: 1
			}, 8, ["open"])
		]));
	}
}), sf = { class: "tater-vue-surface ti-integrations" }, cf = { class: "tv-page-heading" }, lf = { class: "tv-heading-actions" }, uf = { class: "tv-metrics ti-summary" }, df = {
	class: "tv-tabs",
	"aria-label": "Integration sections"
}, ff = ["onClick"], pf = {
	key: 1,
	class: "ti-manager"
}, mf = { class: "tv-mini-tabs" }, hf = ["onClick"], gf = { key: 0 }, _f = {
	key: 0,
	class: "tv-notice error"
}, vf = {
	key: 1,
	class: "ti-card-grid"
}, yf = { class: "tv-eyebrow" }, bf = {
	key: 0,
	class: "ti-version"
}, xf = {
	key: 1,
	class: "ti-tags"
}, Sf = ["onClick"], Cf = { key: 1 }, wf = ["onClick"], Tf = {
	key: 0,
	class: "tv-empty"
}, Ef = {
	key: 2,
	class: "ti-card-grid"
}, Df = { class: "tv-eyebrow" }, Of = { class: "tv-state" }, kf = ["onClick"], Af = {
	key: 0,
	class: "tv-empty"
}, jf = {
	key: 3,
	class: "ti-manage-list"
}, Mf = { class: "ti-manage-toolbar" }, Nf = ["disabled"], Pf = { class: "ti-row-actions" }, Ff = ["disabled", "onClick"], If = ["onClick"], Lf = {
	key: 1,
	class: "ti-purge"
}, Rf = ["onUpdate:modelValue"], zf = ["onClick"], Bf = {
	key: 3,
	class: "tv-state good"
}, Vf = {
	key: 4,
	class: "tv-panel ti-repos"
}, Hf = { class: "ti-repo-row builtin" }, Uf = ["onClick"], Wf = { class: "ti-repo-form" }, Gf = {
	key: 2,
	class: "tv-panel ti-browser"
}, Kf = { class: "tv-panel-head" }, qf = {
	key: 0,
	class: "ti-browser-layout"
}, Jf = ["onClick"], Yf = { class: "ti-device-content" }, Xf = { class: "tv-eyebrow" }, Zf = { class: "tv-state" }, Qf = { class: "ti-tags" }, $f = {
	key: 1,
	class: "tv-empty"
}, ep = {
	key: 3,
	class: "ti-rooms"
}, tp = { class: "tv-panel ti-room-toolbar" }, np = { class: "ti-room-grid" }, rp = { class: "tv-eyebrow" }, ip = {
	key: 0,
	class: "ti-room-controls"
}, ap = ["onUpdate:modelValue"], op = ["onClick"], sp = ["value", "onChange"], cp = ["value"], lp = ["value"], up = { class: "ti-room-devices" }, dp = ["value", "onChange"], fp = ["value"], pp = ["onUpdate:modelValue"], mp = ["onClick"], hp = ["onClick"], gp = {
	key: 0,
	class: "tv-empty compact"
}, _p = {
	key: 4,
	class: "tv-panel ti-activity"
}, vp = { class: "tv-panel-head" }, yp = { class: "tv-metrics" }, bp = { class: "ti-event-list" }, xp = { class: "ti-provider" }, Sp = { class: "tv-state" }, Cp = {
	key: 0,
	class: "tv-empty"
}, wp = { class: "tv-eyebrow" }, Tp = { class: "tv-form-grid" }, Ep = ["onUpdate:modelValue"], Dp = [
	"onUpdate:modelValue",
	"rows",
	"placeholder"
], Op = [
	"onUpdate:modelValue",
	"type",
	"min",
	"max",
	"step",
	"placeholder"
], kp = {
	key: 0,
	class: "ti-modal-actions"
}, Ap = ["onClick"], jp = {
	key: 0,
	class: "tv-button primary",
	type: "submit"
}, Mp = /* @__PURE__ */ ar({
	__name: "IntegrationsApp",
	props: {
		state: {},
		options: {}
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ G([
			"manager",
			"devices",
			"rooms",
			"runtime"
		].includes(t.options.initialTab || "") ? String(t.options.initialTab) : "manager"), r = /* @__PURE__ */ G("installed"), i = /* @__PURE__ */ G(""), a = /* @__PURE__ */ G(""), o = /* @__PURE__ */ G(""), s = /* @__PURE__ */ G(""), c = /* @__PURE__ */ G(null), l = /* @__PURE__ */ G({}), u = /* @__PURE__ */ G({}), d = /* @__PURE__ */ G(""), f = /* @__PURE__ */ G(""), p = /* @__PURE__ */ G([]), m = /* @__PURE__ */ G(t.state.settings.integration_device_registry || {}), h = /* @__PURE__ */ G(t.state.settings.integration_runtime || {}), g = /* @__PURE__ */ G({}), _ = /* @__PURE__ */ G({}), v = /* @__PURE__ */ G(""), y = /* @__PURE__ */ G({}), b = /* @__PURE__ */ G({}), x = 0, S = !1, C = !1, w = !1, T = $(() => t.state.settings || {}), E = $(() => Array.isArray(T.value.integrations) ? T.value.integrations : []), D = $(() => T.value.integration_shop || {}), O = $(() => Array.isArray(D.value.installed) ? D.value.installed : []), k = $(() => Array.isArray(D.value.catalog) ? D.value.catalog.filter((e) => !e.installed) : []), A = $(() => O.value.filter((e) => e.update_available)), ee = $(() => O.value.filter((e) => e.enabled).length || (O.value.length ? 0 : E.value.length)), j = $(() => new Map(E.value.map((e) => [I(e.id), e]))), M = $(() => {
			let e = /* @__PURE__ */ new Set(), t = O.value.map((t) => {
				let n = F(t.id || t.module_key || t.key);
				return e.add(I(n)), {
					id: n,
					shop: t,
					integration: j.value.get(I(n)) || null
				};
			});
			return E.value.forEach((n) => {
				let r = F(n.id);
				r && !e.has(I(r)) && t.push({
					id: r,
					shop: null,
					integration: n
				});
			}), t.sort((e, t) => F(e.integration?.name || e.shop?.name || e.id).localeCompare(F(t.integration?.name || t.shop?.name || t.id)));
		}), te = $(() => Array.isArray(m.value.categories) ? m.value.categories.filter((e) => Number(e.device_count || 0) > 0) : []), N = $(() => te.value.find((e) => F(e.id) === s.value) || te.value[0] || null), ne = $(() => {
			let e = Array.isArray(m.value.rooms) ? m.value.rooms.slice() : [], t = Array.isArray(m.value.room_overrides?.rooms) ? m.value.room_overrides.rooms : [], n = new Set(e.map((e) => F(e.id)));
			return t.forEach((t) => {
				n.has(F(t.id)) || e.push({
					...t,
					devices: [],
					categories: [],
					source: "tater"
				});
			}), e.sort((e, t) => F(e.name).localeCompare(F(t.name)));
		}), re = $(() => Array.isArray(m.value.room_media_player_options) ? m.value.room_media_player_options : []), P = $(() => (Array.isArray(_.value.events) ? _.value.events : []).filter((e) => {
			let t = F(e.kind || e.type).toLowerCase();
			return ![
				"snapshot",
				"poll",
				"heartbeat",
				"runtime_status"
			].some((e) => t.includes(e));
		}).sort((e, t) => Number(t.ts || 0) - Number(e.ts || 0)).slice(0, 40));
		function F(e) {
			return String(e ?? "").trim();
		}
		function I(e) {
			let t = F(e);
			return t === "ecobee_homekit" ? "homekit" : t;
		}
		function L(e) {
			return encodeURIComponent(F(e));
		}
		function ie(e, n = "success") {
			a.value = e, t.options.onToast?.(e, n);
		}
		function ae(e) {
			return F(e.name || e.friendly_name || e.label || e.title || e.id || e.ref || "Device");
		}
		function R(e) {
			return F(e.id || "unassigned") || "unassigned";
		}
		function B(e) {
			return F(e.id || e.ref);
		}
		function V(e) {
			return F(e.integration_id);
		}
		function oe(e, t) {
			let n = e.values && Object.prototype.hasOwnProperty.call(e.values, t.key) ? e.values[t.key] : t.default ?? "";
			return F(t.type).toLowerCase() === "checkbox" ? typeof n == "string" ? [
				"1",
				"true",
				"yes",
				"on"
			].includes(n.trim().toLowerCase()) : !!n : n;
		}
		function se(e) {
			let t = { ...l.value };
			return (Array.isArray(e.fields) ? e.fields : []).forEach((e) => {
				let n = F(e.key);
				n && F(e.type).toLowerCase() === "number" && (t[n] = Number(t[n] ?? e.default ?? 0));
			}), t;
		}
		function H(e) {
			return e.payload && typeof e.payload == "object" ? e.payload : {};
		}
		function ce(e) {
			let t = H(e);
			return F(t.name || t.friendly_name || t.device_name || t.entity_name || t.entity_id || t.ref || e.provider || "Device change");
		}
		function le(e) {
			let t = H(e);
			return F(t.state ?? t.value ?? t.status ?? t.current_state ?? e.kind ?? "changed").replaceAll("_", " ");
		}
		function ue(e) {
			let t = Math.max(0, Date.now() / 1e3 - Number(e || 0));
			return t < 60 ? "now" : t < 3600 ? `${Math.floor(t / 60)}m ago` : t < 86400 ? `${Math.floor(t / 3600)}h ago` : `${Math.floor(t / 86400)}d ago`;
		}
		async function de(e = !1) {
			e || (i.value = "Refreshing integrations…"), o.value = "";
			try {
				t.state.settings = await gs(t.options.endpoints.settings), m.value = t.state.settings.integration_device_registry || m.value, h.value = t.state.settings.integration_runtime || h.value, p.value = Array.isArray(t.state.settings.integration_shop?.repos?.additional) ? t.state.settings.integration_shop.repos.additional.map((e) => ({ ...e })) : [];
			} catch (t) {
				o.value = t instanceof Error ? t.message : "Integration refresh failed.", e || ie(o.value, "error");
			} finally {
				e || (i.value = "");
			}
		}
		async function fe(e, n = "") {
			if (!(e === "remove" && !window.confirm(`Remove ${n}?${u.value[n] ? " Its saved data will also be deleted." : ""}`))) {
				i.value = `${e.replaceAll("-", " ")} ${n || "integrations"}…`, o.value = "";
				try {
					let r = n ? { id: n } : {};
					e === "remove" && (r.purge_redis = !!u.value[n]), ie(F((await _s(`${t.options.endpoints.shop}/${e}`, r)).message) || "Integration action completed."), await de(!0);
				} catch (e) {
					o.value = e instanceof Error ? e.message : "Integration action failed.", ie(o.value, "error");
				} finally {
					i.value = "";
				}
			}
		}
		function pe(e) {
			c.value = e, l.value = Object.fromEntries((Array.isArray(e.fields) ? e.fields : []).map((t) => [F(t.key), oe(e, t)]));
		}
		async function me() {
			let e = c.value;
			if (e) {
				i.value = `Saving ${ae(e)}…`;
				try {
					await _s(`${t.options.endpoints.integrationSettings}/${L(e.id)}/settings`, { settings: se(e) }), ie(`${ae(e)} settings saved.`), c.value = null, await de(!0);
				} catch (e) {
					ie(e instanceof Error ? e.message : "Settings save failed.", "error");
				} finally {
					i.value = "";
				}
			}
		}
		async function W(e) {
			let n = c.value;
			if (n) {
				i.value = F(e.status || `Running ${e.label || e.id}…`);
				try {
					let r = await _s(`${t.options.endpoints.integrationActions}/${L(n.id)}/actions/${L(e.id)}`, { payload: se(n) }), i = r.values && typeof r.values == "object" ? r.values : r, a = new Set((n.fields || []).map((e) => F(e.key)));
					l.value = {
						...l.value,
						...Object.fromEntries(Object.entries(i).filter(([e]) => a.has(e)))
					}, ie(F(r.message) || `${e.label || e.id} complete.`, r.ok === !1 ? "error" : "success");
				} catch (e) {
					ie(e instanceof Error ? e.message : "Integration action failed.", "error");
				} finally {
					i.value = "";
				}
			}
		}
		async function he() {
			i.value = "Saving integration repositories…";
			try {
				await _s(`${t.options.endpoints.shop}/repos`, { repos: p.value }), ie("Integration repositories saved."), await de(!0);
			} catch (e) {
				ie(e instanceof Error ? e.message : "Repository save failed.", "error");
			} finally {
				i.value = "";
			}
		}
		function ge() {
			let e = f.value.trim();
			if (!e) {
				ie("Repo URL is required.", "error");
				return;
			}
			if (p.value.some((t) => F(t.url).toLowerCase() === e.toLowerCase())) {
				ie("That repo is already added.", "error");
				return;
			}
			p.value.push({
				name: d.value.trim(),
				url: e
			}), d.value = "", f.value = "", a.value = "Repo added. Save repositories to apply it.";
		}
		async function _e(e = !1) {
			try {
				let n = await gs(e ? t.options.endpoints.rooms : t.options.endpoints.deviceRegistry);
				m.value = n.registry || n, s.value ||= F(te.value[0]?.id), F(m.value.cache?.source) === "building" && ve();
			} catch (e) {
				ie(e instanceof Error ? e.message : "Device load failed.", "error");
			}
		}
		async function ve() {
			if (!C) {
				C = !0;
				try {
					for (let e = 0; e < 120 && !w; e += 1) {
						await new Promise((e) => window.setTimeout(e, 500));
						let e = await gs(n.value === "rooms" ? t.options.endpoints.rooms : t.options.endpoints.deviceRegistry), r = e.registry || e;
						if (F(r.cache?.source) !== "building") {
							m.value = r, s.value ||= F(te.value[0]?.id);
							return;
						}
					}
				} catch {} finally {
					C = !1;
				}
			}
		}
		async function ye(e = !1) {
			if (!S) {
				S = !0, i.value = e ? "Refreshing rooms in background…" : "Refreshing devices in background…";
				try {
					let e = await _s(`${t.options.endpoints.systemTasks}/integration_device_registry/run`), r = Number(e.task?.run_count || 0);
					for (let e = 0; e < 120 && !w; e += 1) {
						await new Promise((e) => window.setTimeout(e, 500));
						let e = await gs(t.options.endpoints.systemTasks), i = (Array.isArray(e.tasks) ? e.tasks : []).find((e) => F(e.id) === "integration_device_registry");
						if (!(!i || i.running || Number(i.run_count || 0) <= r)) {
							if (F(i.last_error)) throw Error(F(i.last_error));
							await _e(n.value === "rooms"), ie("Integration devices refreshed.");
							return;
						}
					}
					if (!w) throw Error("The integration device refresh is still running. You can follow it in System Tasks.");
				} catch (e) {
					ie(e instanceof Error ? e.message : "Device refresh failed.", "error");
				} finally {
					S = !1, i.value = "";
				}
			}
		}
		async function be(e, n) {
			i.value = "Saving organization changes…";
			try {
				let r = await _s(t.options.endpoints.rooms, {
					action: e,
					payload: n
				});
				m.value = r.registry || r, ie("Organization changes saved.");
			} catch (e) {
				ie(e instanceof Error ? e.message : "Organization update failed.", "error");
			} finally {
				i.value = "";
			}
		}
		async function xe() {
			let e = v.value.trim();
			e && (await be("create_room", { name: e }), v.value = "");
		}
		async function Se(e) {
			let t = F(y.value[R(e)] || e.name);
			t && await be("rename_room", {
				room_id: R(e),
				name: t
			});
		}
		async function Ce(e, t) {
			!t || t === "unassigned" ? await be("clear_device_room", {
				integration_id: V(e),
				device_id: B(e)
			}) : await be("assign_device_room", {
				integration_id: V(e),
				device_id: B(e),
				room_id: t,
				room_name: F(ne.value.find((e) => R(e) === t)?.name)
			});
		}
		async function we(e) {
			let t = F(b.value[`${V(e)}:${B(e)}`] || ae(e));
			t && await be("rename_device", {
				integration_id: V(e),
				device_id: B(e),
				name: t
			});
		}
		async function Te(e, t) {
			t ? await be("set_room_preferred_media_player", {
				room_id: R(e),
				room_name: e.name,
				target: t
			}) : await be("clear_room_preferred_media_player", { room_id: R(e) });
		}
		async function Ee(e = !1) {
			e || (i.value = "Refreshing activity…");
			try {
				let [e, n, r] = await Promise.all([
					gs(t.options.endpoints.runtime),
					gs(t.options.endpoints.runtimeStates),
					gs(`${t.options.endpoints.runtimeEvents}?limit=1000`)
				]);
				h.value = e.runtime || e, g.value = n, _.value = r;
			} catch (t) {
				e || ie(t instanceof Error ? t.message : "Activity refresh failed.", "error");
			} finally {
				e || (i.value = "");
			}
		}
		function De(e) {
			n.value = e, t.options.onTabChange?.(e);
		}
		return En(() => t.state.settings, () => {
			m.value = t.state.settings.integration_device_registry || m.value, h.value = t.state.settings.integration_runtime || h.value;
		}), En(te, (e) => {
			e.some((e) => F(e.id) === s.value) || (s.value = F(e[0]?.id));
		}, { immediate: !0 }), En(ne, (e) => {
			let t = {}, n = {};
			e.forEach((e) => {
				t[R(e)] = F(e.name), (e.devices || []).forEach((e) => {
					n[`${V(e)}:${B(e)}`] = ae(e);
				});
			}), y.value = t, b.value = n;
		}, { immediate: !0 }), En(n, (e) => {
			window.clearInterval(x), e === "devices" && _e(!1), e === "rooms" && _e(!0), e === "runtime" && (Ee(), x = window.setInterval(() => void Ee(!0), 1e4));
		}, { immediate: !0 }), Cr(() => {
			w = !0, window.clearInterval(x);
		}), p.value = Array.isArray(D.value.repos?.additional) ? D.value.repos.additional.map((e) => ({ ...e })) : [], (e, t) => (J(), Y("div", sf, [
			X("header", cf, [t[11] ||= X("div", null, [
				X("span", { class: "tv-eyebrow" }, "Connected home"),
				X("h1", null, "Integrations"),
				X("p", null, "Services, devices, rooms, live state, and Tater Shop updates in one place.")
			], -1), X("div", lf, [X("span", { class: z(["tv-live-pill", { busy: !!i.value }]) }, [t[10] ||= X("i", null, null, -1), Z(U(i.value || "Live"), 1)], 2), X("button", {
				class: "tv-button",
				type: "button",
				onClick: t[0] ||= (e) => de()
			}, "Refresh")])]),
			X("div", uf, [
				X("div", null, [t[12] ||= X("span", null, "Installed", -1), X("strong", null, U(O.value.length || E.value.length), 1)]),
				X("div", null, [t[13] ||= X("span", null, "Enabled", -1), X("strong", null, U(ee.value), 1)]),
				X("div", null, [t[14] ||= X("span", null, "Devices", -1), X("strong", null, U(Number(m.value.total || 0)), 1)]),
				X("div", null, [t[15] ||= X("span", null, "Updates", -1), X("strong", null, U(Number(D.value.updates_available || A.value.length)), 1)])
			]),
			a.value || o.value ? (J(), Y("div", {
				key: 0,
				class: z(["tv-notice", { error: !!o.value }])
			}, U(o.value || a.value), 3)) : Q("", !0),
			X("nav", df, [(J(), Y(q, null, K([
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
			], (e) => X("button", {
				key: e.id,
				type: "button",
				class: z({ active: n.value === e.id }),
				onClick: (t) => De(e.id)
			}, U(e.label), 11, ff)), 64))]),
			n.value === "manager" ? (J(), Y("section", pf, [
				X("nav", mf, [(J(), Y(q, null, K([
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
				], (e) => X("button", {
					key: e.id,
					class: z({ active: r.value === e.id }),
					type: "button",
					onClick: (t) => r.value = e.id
				}, [Z(U(e.label), 1), e.id === "manage" && A.value.length ? (J(), Y("span", gf, U(A.value.length), 1)) : Q("", !0)], 10, hf)), 64))]),
				D.value.errors?.length ? (J(), Y("div", _f, U(D.value.errors.join(" • ")), 1)) : Q("", !0),
				r.value === "installed" ? (J(), Y("div", vf, [(J(!0), Y(q, null, K(M.value, (e) => (J(), Y("article", {
					key: e.id,
					class: "tv-panel ti-integration-card"
				}, [
					X("header", null, [X("div", null, [X("span", yf, U(e.id), 1), X("h2", null, U(e.integration?.name || e.shop?.name || e.id), 1)]), X("span", { class: z(["tv-state", { good: e.shop?.enabled !== !1 }]) }, U(e.shop?.enabled === !1 ? "Disabled" : "Enabled"), 3)]),
					X("p", null, U(e.integration?.description || e.shop?.description || "Connected integration."), 1),
					e.shop ? (J(), Y("div", bf, [Z("Installed " + U(e.shop.installed_ver || "0.0.0") + " ", 1), X("span", null, "Store " + U(e.shop.store_ver || "-"), 1)])) : Q("", !0),
					e.integration?.capabilities?.length ? (J(), Y("div", xf, [(J(!0), Y(q, null, K(e.integration.capabilities, (e) => (J(), Y("span", { key: e }, U(e), 1))), 128))])) : Q("", !0),
					X("footer", null, [e.integration && (e.integration.fields?.length || e.integration.actions?.length) ? (J(), Y("button", {
						key: 0,
						class: "tv-button",
						type: "button",
						onClick: (t) => pe(e.integration)
					}, "Settings", 8, Sf)) : (J(), Y("span", Cf, "No configurable settings")), e.shop && !e.shop.required ? (J(), Y("button", {
						key: 2,
						class: "tv-button",
						type: "button",
						onClick: (t) => fe(e.shop.enabled ? "disable" : "enable", e.id)
					}, U(e.shop.enabled ? "Disable" : "Enable"), 9, wf)) : Q("", !0)])
				]))), 128)), M.value.length ? Q("", !0) : (J(), Y("div", Tf, "No installed integrations found."))])) : r.value === "store" ? (J(), Y("div", Ef, [(J(!0), Y(q, null, K(k.value, (e) => (J(), Y("article", {
					key: e.id,
					class: "tv-panel ti-integration-card"
				}, [
					X("header", null, [X("div", null, [X("span", Df, U(e.id), 1), X("h2", null, U(e.name || e.id), 1)]), X("span", Of, "v" + U(e.version || "-"), 1)]),
					X("p", null, U(e.description), 1),
					X("footer", null, [X("span", null, U(e.source_label || "Tater Shop"), 1), X("button", {
						class: "tv-button primary",
						type: "button",
						onClick: (t) => fe("install", e.id)
					}, "Download", 8, kf)])
				]))), 128)), k.value.length ? Q("", !0) : (J(), Y("div", Af, "No additional integrations are available."))])) : r.value === "manage" ? (J(), Y("div", jf, [X("div", Mf, [X("div", null, [t[16] ||= X("h2", null, "Manage installed integrations", -1), X("p", null, U(A.value.length) + " update" + U(A.value.length === 1 ? "" : "s") + " available.", 1)]), X("button", {
					class: "tv-button primary",
					type: "button",
					disabled: !A.value.length,
					onClick: t[1] ||= (e) => fe("update-all")
				}, "Update all", 8, Nf)]), (J(!0), Y(q, null, K(O.value, (e) => (J(), Y("article", {
					key: e.id,
					class: "tv-panel ti-manage-row"
				}, [X("div", null, [X("strong", null, U(e.name || e.id), 1), X("span", null, U(e.installed_ver || "0.0.0") + " → " + U(e.store_ver || "-"), 1)]), X("div", Pf, [
					X("button", {
						class: "tv-button",
						type: "button",
						disabled: !e.update_available,
						onClick: (t) => fe("update", e.id)
					}, U(e.update_available ? "Update" : "Current"), 9, Ff),
					e.required ? Q("", !0) : (J(), Y("button", {
						key: 0,
						class: "tv-button",
						type: "button",
						onClick: (t) => fe(e.enabled ? "disable" : "enable", e.id)
					}, U(e.enabled ? "Disable" : "Enable"), 9, If)),
					e.required ? Q("", !0) : (J(), Y("label", Lf, [bn(X("input", {
						"onUpdate:modelValue": (t) => u.value[e.id] = t,
						type: "checkbox"
					}, null, 8, Rf), [[Jo, u.value[e.id]]]), t[17] ||= Z(" Delete data", -1)])),
					e.required ? (J(), Y("span", Bf, "Required")) : (J(), Y("button", {
						key: 2,
						class: "tv-button danger",
						type: "button",
						onClick: (t) => fe("remove", e.id)
					}, "Remove", 8, zf))
				])]))), 128))])) : (J(), Y("div", Vf, [
					t[21] ||= X("header", null, [X("div", null, [
						X("span", { class: "tv-eyebrow" }, "Sources"),
						X("h2", null, "Integration repositories"),
						X("p", null, "The built-in repository stays available; add trusted sources below.")
					])], -1),
					X("article", Hf, [X("div", null, [X("strong", null, U(D.value.repos?.default?.name || "Default"), 1), X("code", null, U(D.value.repos?.default?.url || "(not set)"), 1)]), t[18] ||= X("span", null, "Built-in", -1)]),
					(J(!0), Y(q, null, K(p.value, (e, t) => (J(), Y("article", {
						key: `${e.url}-${t}`,
						class: "ti-repo-row"
					}, [X("div", null, [X("strong", null, U(e.name || "Additional repo"), 1), X("code", null, U(e.url), 1)]), X("button", {
						class: "tv-button",
						type: "button",
						onClick: (e) => p.value.splice(t, 1)
					}, "Remove", 8, Uf)]))), 128)),
					X("div", Wf, [
						X("label", null, [t[19] ||= X("span", null, "Name (optional)", -1), bn(X("input", {
							"onUpdate:modelValue": t[2] ||= (e) => d.value = e,
							type: "text",
							placeholder: "My Integration Repo"
						}, null, 512), [[qo, d.value]])]),
						X("label", null, [t[20] ||= X("span", null, "Repo URL", -1), bn(X("input", {
							"onUpdate:modelValue": t[3] ||= (e) => f.value = e,
							type: "url",
							placeholder: "https://example.com/integrations.json",
							onKeyup: cs(ge, ["enter"])
						}, null, 544), [[qo, f.value]])]),
						X("button", {
							class: "tv-button",
							type: "button",
							onClick: ge
						}, "Add"),
						X("button", {
							class: "tv-button primary",
							type: "button",
							onClick: he
						}, "Save repositories")
					])
				]))
			])) : n.value === "devices" ? (J(), Y("section", Gf, [X("header", Kf, [t[22] ||= X("div", null, [
				X("span", { class: "tv-eyebrow" }, "Device registry"),
				X("h2", null, "Browse devices"),
				X("p", null, "Grouped by category, room, and integration.")
			], -1), X("button", {
				class: "tv-button",
				type: "button",
				onClick: t[4] ||= (e) => ye(!1)
			}, "Refresh devices")]), te.value.length ? (J(), Y("div", qf, [X("aside", null, [(J(!0), Y(q, null, K(te.value, (e) => (J(), Y("button", {
				key: e.id,
				type: "button",
				class: z({ active: N.value?.id === e.id }),
				onClick: (t) => s.value = F(e.id)
			}, [X("strong", null, U(e.name), 1), X("span", null, U(e.device_count) + " devices · " + U(e.room_count) + " rooms", 1)], 10, Jf))), 128))]), X("div", Yf, [X("header", null, [X("div", null, [
				X("span", Xf, U(N.value?.id), 1),
				X("h2", null, U(N.value?.name), 1),
				X("p", null, U(N.value?.description), 1)
			])]), (J(!0), Y(q, null, K(N.value?.rooms || [], (e) => (J(), Y("div", {
				key: e.id,
				class: "ti-device-room"
			}, [X("div", null, [X("strong", null, U(e.name), 1), X("span", null, U(e.devices?.length || 0) + " devices", 1)]), (J(!0), Y(q, null, K(e.devices || [], (e) => (J(), Y("article", {
				key: B(e),
				class: "ti-device-row"
			}, [
				X("div", null, [X("strong", null, U(ae(e)), 1), X("span", null, U([
					e.integration_name || e.integration_id,
					e.type,
					e.ref || e.id
				].filter(Boolean).join(" / ")), 1)]),
				X("div", null, [X("span", Zf, U(e.state || e.status || "unknown"), 1), X("small", null, U(e.room || e.area || "Unassigned"), 1)]),
				X("div", Qf, [(J(!0), Y(q, null, K((e.features?.length ? e.features : e.actions || e.capabilities || []).slice(0, 6), (e) => (J(), Y("span", { key: e }, U(F(e).replaceAll("_", " ")), 1))), 128))])
			]))), 128))]))), 128))])])) : (J(), Y("div", $f, "No devices are available from enabled integrations yet."))])) : n.value === "rooms" ? (J(), Y("section", ep, [X("div", tp, [t[23] ||= X("div", null, [
				X("span", { class: "tv-eyebrow" }, "Organization"),
				X("h2", null, "Rooms and device names"),
				X("p", null, "Set Tater-friendly names, room assignments, and preferred playback targets.")
			], -1), X("div", null, [
				bn(X("input", {
					"onUpdate:modelValue": t[5] ||= (e) => v.value = e,
					type: "text",
					placeholder: "New room name",
					onKeyup: cs(xe, ["enter"])
				}, null, 544), [[qo, v.value]]),
				X("button", {
					class: "tv-button primary",
					type: "button",
					onClick: xe
				}, "Create room"),
				X("button", {
					class: "tv-button",
					type: "button",
					onClick: t[6] ||= (e) => ye(!0)
				}, "Refresh")
			])]), X("div", np, [(J(!0), Y(q, null, K(ne.value, (e) => (J(), Y("article", {
				key: R(e),
				class: "tv-panel ti-room-card"
			}, [
				X("header", null, [X("div", null, [X("span", rp, U(e.source || "integration"), 1), X("h2", null, U(e.name || "Unassigned"), 1)]), X("span", null, U(e.devices?.length || 0) + " devices", 1)]),
				R(e) === "unassigned" ? Q("", !0) : (J(), Y("div", ip, [X("label", null, [t[24] ||= X("span", null, "Room name", -1), X("div", null, [bn(X("input", {
					"onUpdate:modelValue": (t) => y.value[R(e)] = t,
					type: "text"
				}, null, 8, ap), [[qo, y.value[R(e)]]]), X("button", {
					class: "tv-button",
					type: "button",
					onClick: (t) => Se(e)
				}, "Rename", 8, op)])]), X("label", null, [t[26] ||= X("span", null, "Preferred player", -1), X("select", {
					value: e.preferred_media_player || "",
					onChange: (t) => Te(e, t.target.value)
				}, [
					t[25] ||= X("option", { value: "" }, "Auto", -1),
					e.preferred_media_player && !re.value.some((t) => F(t.value) === F(e.preferred_media_player)) ? (J(), Y("option", {
						key: 0,
						value: e.preferred_media_player
					}, U(e.preferred_media_player) + " (saved)", 9, cp)) : Q("", !0),
					(J(!0), Y(q, null, K(re.value, (e) => (J(), Y("option", {
						key: e.value,
						value: e.value
					}, U(e.label || e.value), 9, lp))), 128))
				], 40, sp)])])),
				X("div", up, [(J(!0), Y(q, null, K(e.devices || [], (n) => (J(), Y("article", { key: `${V(n)}:${B(n)}` }, [
					X("div", null, [X("strong", null, U(ae(n)), 1), X("span", null, U(n.integration_name || n.integration_id) + " · " + U(n.type || "device"), 1)]),
					X("label", null, [t[28] ||= X("span", null, "Room", -1), X("select", {
						value: n.room_id || R(e),
						onChange: (e) => Ce(n, e.target.value)
					}, [t[27] ||= X("option", { value: "unassigned" }, "Unassigned", -1), (J(!0), Y(q, null, K(ne.value.filter((e) => R(e) !== "unassigned"), (e) => (J(), Y("option", {
						key: R(e),
						value: R(e)
					}, U(e.name), 9, fp))), 128))], 40, dp)]),
					X("label", null, [t[29] ||= X("span", null, "Tater name", -1), X("div", null, [
						bn(X("input", {
							"onUpdate:modelValue": (e) => b.value[`${V(n)}:${B(n)}`] = e,
							type: "text"
						}, null, 8, pp), [[qo, b.value[`${V(n)}:${B(n)}`]]]),
						X("button", {
							class: "tv-button",
							type: "button",
							onClick: (e) => we(n)
						}, "Save", 8, mp),
						n.device_name_source === "tater_override" ? (J(), Y("button", {
							key: 0,
							class: "tv-button",
							type: "button",
							onClick: (e) => be("clear_device_name", {
								integration_id: V(n),
								device_id: B(n)
							})
						}, "Use integration", 8, hp)) : Q("", !0)
					])])
				]))), 128)), e.devices?.length ? Q("", !0) : (J(), Y("div", gp, "No devices assigned."))])
			]))), 128))])])) : (J(), Y("section", _p, [
				X("header", vp, [t[30] ||= X("div", null, [
					X("span", { class: "tv-eyebrow" }, "Live integrations"),
					X("h2", null, "Activity"),
					X("p", null, "Connection health and recent device-level changes.")
				], -1), X("button", {
					class: "tv-button",
					type: "button",
					onClick: t[7] ||= (e) => Ee()
				}, "Refresh")]),
				X("div", yp, [
					(J(!0), Y(q, null, K(h.value.enabled_integrations || [], (e) => (J(), Y("div", { key: e }, [X("span", null, U(F(e).replaceAll("_", " ")), 1), X("strong", null, U(h.value[`${e}_ws_connected`] || h.value[`${e}_connected`] ? "Connected" : "Enabled"), 1)]))), 128)),
					X("div", null, [t[31] ||= X("span", null, "Events", -1), X("strong", null, U(h.value.last_event_seq || 0), 1)]),
					X("div", null, [t[32] ||= X("span", null, "Tracked states", -1), X("strong", null, U(g.value.count || h.value.state_count || 0), 1)])
				]),
				X("div", bp, [(J(!0), Y(q, null, K(P.value, (e) => (J(), Y("article", { key: e.seq }, [
					X("span", xp, U(F(e.provider).replaceAll("_", " ")), 1),
					X("div", null, [X("strong", null, U(ce(e)), 1), X("small", null, U(H(e).room || H(e).area || H(e).entity_id || H(e).ref || ""), 1)]),
					X("span", Sp, U(le(e)), 1),
					X("time", null, U(ue(e.ts)), 1)
				]))), 128)), P.value.length ? Q("", !0) : (J(), Y("div", Cp, "No recent device changes in the current activity window."))])
			])),
			aa(sl, {
				open: !!c.value,
				onClose: t[9] ||= (e) => c.value = null
			}, {
				default: yn(() => [X("form", {
					class: "tv-modal",
					onSubmit: os(me, ["prevent"])
				}, [
					X("header", null, [X("div", null, [X("span", wp, U(c.value?.id), 1), X("h2", null, U(c.value ? ae(c.value) : "") + " settings", 1)]), X("button", {
						class: "tv-button",
						type: "button",
						onClick: t[8] ||= (e) => c.value = null
					}, "Close")]),
					X("div", Tp, [(J(!0), Y(q, null, K(c.value?.fields || [], (e) => (J(), Y("label", {
						key: e.key,
						class: z({ full: e.full_width || e.type === "textarea" })
					}, [
						X("span", null, U(e.label || e.key), 1),
						e.type === "checkbox" ? bn((J(), Y("input", {
							key: 0,
							"onUpdate:modelValue": (t) => l.value[e.key] = t,
							class: "tv-checkbox",
							type: "checkbox"
						}, null, 8, Ep)), [[Jo, l.value[e.key]]]) : e.type === "textarea" ? bn((J(), Y("textarea", {
							key: 1,
							"onUpdate:modelValue": (t) => l.value[e.key] = t,
							rows: e.rows || 3,
							placeholder: e.placeholder
						}, null, 8, Dp)), [[qo, l.value[e.key]]]) : bn((J(), Y("input", {
							key: 2,
							"onUpdate:modelValue": (t) => l.value[e.key] = t,
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
						}, null, 8, Op)), [[ts, l.value[e.key]]]),
						X("small", null, U(e.description), 1)
					], 2))), 128))]),
					c.value?.actions?.length ? (J(), Y("div", kp, [t[33] ||= X("span", null, "Actions", -1), (J(!0), Y(q, null, K(c.value?.actions || [], (e) => (J(), Y("button", {
						key: e.id,
						class: "tv-button",
						type: "button",
						onClick: (t) => W(e)
					}, U(e.label || e.id), 9, Ap))), 128))])) : Q("", !0),
					X("footer", null, [X("span", null, U(i.value || a.value), 1), c.value?.fields?.length ? (J(), Y("button", jp, "Save settings")) : Q("", !0)])
				], 32)]),
				_: 1
			}, 8, ["open"])
		]));
	}
}), Np = { class: "tater-vue-surface tp-portals" }, Pp = { class: "tv-page-heading" }, Fp = { class: "tv-heading-actions" }, Ip = { class: "tv-metrics" }, Lp = {
	key: 1,
	class: "tv-notice error"
}, Rp = {
	class: "tv-tabs tp-tabs",
	"aria-label": "Portal sections"
}, zp = ["onClick"], Bp = { key: 0 }, Vp = {
	key: 2,
	class: "tp-card-grid"
}, Hp = { class: "tv-eyebrow" }, Up = { class: "tp-version" }, Wp = ["onClick"], Gp = { key: 1 }, Kp = ["onClick"], qp = {
	key: 0,
	class: "tv-empty"
}, Jp = {
	key: 3,
	class: "tp-card-grid"
}, Yp = { class: "tv-eyebrow" }, Xp = { class: "tv-state" }, Zp = ["onClick"], Qp = {
	key: 0,
	class: "tv-empty"
}, $p = {
	key: 4,
	class: "tp-manage-list"
}, em = { class: "tv-panel tp-manage-toolbar" }, tm = ["disabled"], nm = { class: "ti-row-actions" }, rm = ["disabled", "onClick"], im = ["onClick"], am = { class: "ti-purge" }, om = ["onUpdate:modelValue"], sm = ["onClick"], cm = {
	key: 0,
	class: "tv-empty"
}, lm = {
	key: 5,
	class: "tv-panel tp-repos"
}, um = { class: "ti-repo-row builtin" }, dm = ["onClick"], fm = {
	key: 0,
	class: "tv-empty compact"
}, pm = { class: "tp-repo-form" }, mm = { class: "tv-eyebrow" }, hm = { class: "tvb-field-grid" }, gm = /* @__PURE__ */ ar({
	__name: "PortalsApp",
	props: {
		state: {},
		options: {}
	},
	setup(e) {
		let t = e, n = [
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
		], r = /* @__PURE__ */ G(n.some((e) => e.id === t.options.initialTab) ? String(t.options.initialTab) : "installed"), i = /* @__PURE__ */ G(""), a = /* @__PURE__ */ G(""), o = /* @__PURE__ */ G(""), s = /* @__PURE__ */ G({}), c = /* @__PURE__ */ G(""), l = /* @__PURE__ */ G(""), u = /* @__PURE__ */ G([]), d = /* @__PURE__ */ G(null), f = /* @__PURE__ */ G({}), p = $(() => t.state.payload?.runtime || {}), m = $(() => t.state.payload?.shop || {}), h = $(() => Array.isArray(p.value.items) ? p.value.items : []), g = $(() => Array.isArray(m.value.installed) ? m.value.installed : []), _ = $(() => Array.isArray(m.value.catalog) ? m.value.catalog : []), v = $(() => _.value.filter((e) => !e.installed).sort(O)), y = $(() => g.value.filter((e) => e.update_available)), b = $(() => h.value.filter((e) => !!e.running).length), x = $(() => new Map(h.value.map((e) => [E(e.key), e]))), S = $(() => {
			let e = /* @__PURE__ */ new Map();
			return g.value.forEach((t) => {
				let n = w(t.module_key || `${t.id}_portal`);
				n && e.set(E(n), t), t.id && e.set(E(t.id), t);
			}), e;
		}), C = $(() => {
			let e = /* @__PURE__ */ new Set(), t = h.value.map((t) => {
				let n = w(t.key), r = S.value.get(E(n)) || S.value.get(E(D(n))) || null;
				return r && e.add(E(r.id)), {
					key: n,
					runtime: t,
					shop: r
				};
			});
			return g.value.forEach((n) => {
				e.has(E(n.id)) || t.push({
					key: w(n.module_key || `${n.id}_portal`),
					runtime: null,
					shop: n
				});
			}), t.sort((e, t) => k(e).localeCompare(k(t), void 0, {
				sensitivity: "base",
				numeric: !0
			}));
		});
		function w(e) {
			return String(e ?? "").trim();
		}
		function T(e) {
			return encodeURIComponent(w(e));
		}
		function E(e) {
			return w(e).toLowerCase();
		}
		function D(e) {
			return w(e).replace(/_portal$/i, "");
		}
		function O(e, t) {
			return w(e.name || e.id).localeCompare(w(t.name || t.id), void 0, {
				sensitivity: "base",
				numeric: !0
			});
		}
		function k(e) {
			return w(e.runtime?.label || e.shop?.name || D(e.key));
		}
		function A(e) {
			return w(e.shop?.description || "Local Portal module.");
		}
		function ee(e) {
			let t = w(e.module_key || `${e.id}_portal`);
			return x.value.get(E(t)) || x.value.get(E(e.id)) || null;
		}
		function j(e) {
			return e ? e.running ? "Running" : e.desired_running ? "Pending start" : "Stopped" : "Unavailable";
		}
		function M(e, n = "success") {
			a.value = e, o.value = n === "error" ? e : "", t.options.onToast?.(e, n);
		}
		function te() {
			u.value = Array.isArray(m.value.repos?.additional) ? m.value.repos.additional.map((e) => ({ ...e })) : [];
		}
		async function N(e = !1) {
			e || (i.value = "Refreshing Portals…"), o.value = "";
			try {
				let [e, n] = await Promise.all([gs(t.options.endpoints.runtime), gs(t.options.endpoints.shop)]);
				t.state.payload = {
					runtime: e,
					shop: n
				}, te();
			} catch (e) {
				M(e instanceof Error ? e.message : "Portal refresh failed.", "error");
			} finally {
				e || (i.value = "");
			}
		}
		async function ne(e, n) {
			let r = w(e.key);
			if (r) {
				i.value = `${n === "start" ? "Starting" : "Stopping"} ${r}…`;
				try {
					await _s(`${t.options.endpoints.runtime}/${T(r)}/${n}`), M(`${r} ${n === "start" ? "started" : "stopped"}.`), await N(!0), t.options.onHealthRefresh?.();
				} catch (e) {
					M(e instanceof Error ? e.message : `Portal ${n} failed.`, "error");
				} finally {
					i.value = "";
				}
			}
		}
		async function re(e, n = "") {
			if (!(e === "remove" && !window.confirm(`Remove ${n}?${s.value[n] ? " Its saved data will also be deleted." : ""}`))) {
				i.value = `${e.replaceAll("-", " ")} ${n || "Portals"}…`, o.value = "";
				try {
					let i = n ? { id: n } : {};
					e === "remove" && (i.purge_redis = !!s.value[n]);
					let a = await _s(`${t.options.endpoints.shop}/${e}`, i), o = Array.isArray(a.updated) ? a.updated.length : 0, c = Array.isArray(a.failed) ? a.failed.length : 0, l = e === "update-all" ? `Update-all completed. Updated ${o}, failed ${c}.` : "Portal action completed.";
					M(w(a.message) || l, c ? "error" : "success"), await N(!0), e === "install" && (r.value = "installed"), t.options.onHealthRefresh?.();
				} catch (e) {
					M(e instanceof Error ? e.message : "Portal action failed.", "error");
				} finally {
					i.value = "";
				}
			}
		}
		function P(e) {
			let t = e.value ?? e.default ?? "", n = w(e.type).toLowerCase();
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
				let e = w(t);
				if (!e) return [];
				try {
					let t = JSON.parse(e);
					if (Array.isArray(t)) return t;
				} catch {}
				return e.split(",").map((e) => e.trim()).filter(Boolean);
			}
			return t;
		}
		function F(e) {
			return (Array.isArray(e.show_when_all) ? e.show_when_all : e.show_when && typeof e.show_when == "object" ? [e.show_when] : []).every((e) => {
				let t = w(e.source_key ?? e.key);
				if (!t) return !0;
				let n = [
					...e.any_of || [],
					...e.values || [],
					...e.equals === void 0 ? [] : [e.equals],
					...e.value === void 0 ? [] : [e.value]
				].map((e) => String(e ?? "").trim());
				if (!n.length) return !0;
				let r = typeof f.value[t] == "boolean" ? f.value[t] ? "true" : "false" : String(f.value[t] ?? "").trim();
				return n.includes(r);
			});
		}
		function I(e) {
			d.value = e, f.value = Object.fromEntries((Array.isArray(e.settings) ? e.settings : []).filter((e) => w(e.key)).map((e) => [w(e.key), P(e)]));
		}
		async function L() {
			let e = d.value;
			if (!e) return;
			let n = w(e.key);
			i.value = `Saving ${w(e.label || n)}…`;
			try {
				let r = Object.fromEntries((e.settings || []).filter((e) => {
					let t = w(e.type).toLowerCase();
					return w(e.key) && ![
						"section",
						"header",
						"readonly",
						"read_only",
						"led_preview"
					].includes(t) && F(e);
				}).map((e) => [w(e.key), f.value[w(e.key)]]));
				await _s(`${t.options.endpoints.runtime}/${T(n)}/settings`, { values: r }), M(`Saved settings for ${w(e.label || n)}.`), d.value = null, await N(!0);
			} catch (e) {
				M(e instanceof Error ? e.message : "Portal settings save failed.", "error");
			} finally {
				i.value = "";
			}
		}
		function ie() {
			let e = l.value.trim();
			if (!e) {
				M("Repository URL is required.", "error");
				return;
			}
			if (u.value.some((t) => w(t.url).toLowerCase() === e.toLowerCase())) {
				M("That repository is already added.", "error");
				return;
			}
			u.value.push({
				name: c.value.trim(),
				url: e
			}), c.value = "", l.value = "", a.value = "Repository added. Save repositories to apply it.", o.value = "";
		}
		async function ae() {
			i.value = "Saving Portal repositories…";
			try {
				await _s(`${t.options.endpoints.shop}/repos`, { repos: u.value }), M("Portal repositories saved."), await N(!0);
			} catch (e) {
				M(e instanceof Error ? e.message : "Repository save failed.", "error");
			} finally {
				i.value = "";
			}
		}
		function R(e) {
			e.key === "Escape" && (d.value = null);
		}
		return En(() => t.state.payload, te, { deep: !1 }), te(), window.addEventListener("keydown", R), Cr(() => window.removeEventListener("keydown", R)), (e, t) => (J(), Y("div", Np, [
			X("header", Pp, [t[8] ||= X("div", null, [
				X("span", { class: "tv-eyebrow" }, "Conversation surfaces"),
				X("h1", null, "Portals"),
				X("p", null, "Manage where Tater listens and responds, along with every Portal’s runtime and updates.")
			], -1), X("div", Fp, [X("span", { class: z(["tv-live-pill", { busy: !!i.value }]) }, [t[7] ||= X("i", null, null, -1), Z(U(i.value || "Live"), 1)], 2), X("button", {
				class: "tv-button",
				type: "button",
				onClick: t[0] ||= (e) => N()
			}, "Refresh")])]),
			X("div", Ip, [
				X("div", null, [t[9] ||= X("span", null, "Installed", -1), X("strong", null, U(g.value.length || h.value.length), 1)]),
				X("div", null, [t[10] ||= X("span", null, "Running", -1), X("strong", null, U(b.value), 1)]),
				X("div", null, [t[11] ||= X("span", null, "Store", -1), X("strong", null, U(_.value.length), 1)]),
				X("div", null, [t[12] ||= X("span", null, "Updates", -1), X("strong", null, U(Number(m.value.updates_available || y.value.length)), 1)])
			]),
			a.value || o.value ? (J(), Y("div", {
				key: 0,
				class: z(["tv-notice", { error: !!o.value }])
			}, U(o.value || a.value), 3)) : Q("", !0),
			m.value.errors?.length ? (J(), Y("div", Lp, U(m.value.errors.join(" • ")), 1)) : Q("", !0),
			X("nav", Rp, [(J(), Y(q, null, K(n, (e) => X("button", {
				key: e.id,
				type: "button",
				class: z({ active: r.value === e.id }),
				onClick: (t) => r.value = e.id
			}, [Z(U(e.label), 1), e.id === "manage" && y.value.length ? (J(), Y("span", Bp, U(y.value.length), 1)) : Q("", !0)], 10, zp)), 64))]),
			r.value === "installed" ? (J(), Y("section", Vp, [(J(!0), Y(q, null, K(C.value, (e) => (J(), Y("article", {
				key: e.key,
				class: "tv-panel tp-portal-card"
			}, [
				X("header", null, [X("div", null, [X("span", Hp, U(e.key), 1), X("h2", null, U(k(e)), 1)]), X("span", { class: z(["tv-state", {
					good: e.runtime?.running,
					pending: e.runtime?.desired_running && !e.runtime?.running
				}]) }, U(j(e.runtime)), 3)]),
				X("p", null, U(A(e)), 1),
				X("div", Up, [
					X("span", null, "Installed " + U(e.shop?.installed_ver || "0.0.0"), 1),
					X("span", null, "Store " + U(e.shop?.store_ver || "-"), 1),
					X("span", null, U(e.shop?.source_label || "local"), 1)
				]),
				X("footer", null, [e.runtime?.settings?.length ? (J(), Y("button", {
					key: 0,
					class: "tv-button",
					type: "button",
					onClick: (t) => I(e.runtime)
				}, "Settings", 8, Wp)) : (J(), Y("span", Gp, U(e.runtime ? "No configurable settings" : "Runtime unavailable"), 1)), e.runtime ? (J(), Y("button", {
					key: 2,
					class: z(["tv-button", { primary: !e.runtime.running }]),
					type: "button",
					onClick: (t) => ne(e.runtime, e.runtime.running ? "stop" : "start")
				}, U(e.runtime.running ? "Stop" : "Start"), 11, Kp)) : Q("", !0)])
			]))), 128)), C.value.length ? Q("", !0) : (J(), Y("div", qp, "No installed Portals found."))])) : r.value === "store" ? (J(), Y("section", Jp, [(J(!0), Y(q, null, K(v.value, (e) => (J(), Y("article", {
				key: e.id,
				class: "tv-panel tp-portal-card"
			}, [
				X("header", null, [X("div", null, [X("span", Yp, U(e.id), 1), X("h2", null, U(e.name || e.id), 1)]), X("span", Xp, "v" + U(e.version || "-"), 1)]),
				X("p", null, U(e.description || "No description provided."), 1),
				X("footer", null, [X("span", null, U(e.source_label || "Tater Shop"), 1), X("button", {
					class: "tv-button primary",
					type: "button",
					onClick: (t) => re("install", e.id)
				}, "Install", 8, Zp)])
			]))), 128)), v.value.length ? Q("", !0) : (J(), Y("div", Qp, "No additional Portals are available from the configured repositories."))])) : r.value === "manage" ? (J(), Y("section", $p, [
				X("div", em, [X("div", null, [
					t[13] ||= X("span", { class: "tv-eyebrow" }, "Maintenance", -1),
					t[14] ||= X("h2", null, "Manage installed Portals", -1),
					X("p", null, U(y.value.length) + " update" + U(y.value.length === 1 ? "" : "s") + " available. Running Portals restart automatically after an update.", 1)
				]), X("button", {
					class: "tv-button primary",
					type: "button",
					disabled: !y.value.length,
					onClick: t[1] ||= (e) => re("update-all")
				}, "Update all", 8, tm)]),
				(J(!0), Y(q, null, K(g.value.slice().sort(O), (e) => (J(), Y("article", {
					key: e.id,
					class: "tv-panel tp-manage-row"
				}, [X("div", null, [X("strong", null, U(e.name || e.id), 1), X("span", null, U(e.installed_ver || "0.0.0") + " → " + U(e.store_ver || "-") + " · " + U(j(ee(e))), 1)]), X("div", nm, [
					X("button", {
						class: "tv-button",
						type: "button",
						disabled: !e.update_available,
						onClick: (t) => re("update", e.id)
					}, U(e.update_available ? "Update" : "Current"), 9, rm),
					ee(e) ? (J(), Y("button", {
						key: 0,
						class: "tv-button",
						type: "button",
						onClick: (t) => ne(ee(e), ee(e)?.running ? "stop" : "start")
					}, U(ee(e)?.running ? "Stop" : "Start"), 9, im)) : Q("", !0),
					X("label", am, [bn(X("input", {
						"onUpdate:modelValue": (t) => s.value[e.id] = t,
						type: "checkbox"
					}, null, 8, om), [[Jo, s.value[e.id]]]), t[15] ||= Z(" Delete data", -1)]),
					X("button", {
						class: "tv-button danger",
						type: "button",
						onClick: (t) => re("remove", e.id)
					}, "Remove", 8, sm)
				])]))), 128)),
				g.value.length ? Q("", !0) : (J(), Y("div", cm, "No installed Portals found."))
			])) : (J(), Y("section", lm, [
				t[19] ||= X("header", null, [X("div", null, [
					X("span", { class: "tv-eyebrow" }, "Trusted sources"),
					X("h2", null, "Portal repositories"),
					X("p", null, "The built-in Portal repository stays available. Add other trusted manifests below.")
				])], -1),
				X("article", um, [X("div", null, [X("strong", null, U(m.value.repos?.default?.name || "Default"), 1), X("code", null, U(m.value.repos?.default?.url || "(not set)"), 1)]), t[16] ||= X("span", null, "Built-in", -1)]),
				(J(!0), Y(q, null, K(u.value, (e, t) => (J(), Y("article", {
					key: `${e.url}-${t}`,
					class: "ti-repo-row"
				}, [X("div", null, [X("strong", null, U(e.name || "Additional repository"), 1), X("code", null, U(e.url), 1)]), X("button", {
					class: "tv-button",
					type: "button",
					onClick: (e) => u.value.splice(t, 1)
				}, "Remove", 8, dm)]))), 128)),
				u.value.length ? Q("", !0) : (J(), Y("div", fm, "No additional repositories configured.")),
				X("div", pm, [
					X("label", null, [t[17] ||= X("span", null, "Name (optional)", -1), bn(X("input", {
						"onUpdate:modelValue": t[2] ||= (e) => c.value = e,
						type: "text",
						placeholder: "My Portal Repo"
					}, null, 512), [[qo, c.value]])]),
					X("label", null, [t[18] ||= X("span", null, "Repository URL", -1), bn(X("input", {
						"onUpdate:modelValue": t[3] ||= (e) => l.value = e,
						type: "url",
						placeholder: "https://example.com/portals.json",
						onKeyup: cs(ie, ["enter"])
					}, null, 544), [[qo, l.value]])]),
					X("button", {
						class: "tv-button",
						type: "button",
						onClick: ie
					}, "Add"),
					X("button", {
						class: "tv-button primary",
						type: "button",
						onClick: ae
					}, "Save repositories")
				])
			])),
			aa(sl, {
				open: !!d.value,
				onClose: t[6] ||= (e) => d.value = null
			}, {
				default: yn(() => [X("form", {
					class: "tv-modal tp-settings-modal",
					onSubmit: os(L, ["prevent"])
				}, [
					X("header", null, [X("div", null, [X("span", mm, U(d.value?.key), 1), X("h2", null, U(d.value?.label || d.value?.key) + " settings", 1)]), X("button", {
						class: "tv-button",
						type: "button",
						onClick: t[4] ||= (e) => d.value = null
					}, "Close")]),
					X("div", hm, [(J(!0), Y(q, null, K(d.value?.settings || [], (e, n) => (J(), ea(Ru, {
						key: e.key || n,
						modelValue: f.value[e.key],
						"onUpdate:modelValue": (t) => f.value[e.key] = t,
						field: e,
						"all-values": f.value,
						onError: t[5] ||= (e) => M(e, "error"),
						onNotify: M
					}, null, 8, [
						"modelValue",
						"onUpdate:modelValue",
						"field",
						"all-values"
					]))), 128))]),
					X("footer", null, [X("span", null, U(i.value || a.value), 1), t[20] ||= X("button", {
						class: "tv-button primary",
						type: "submit"
					}, "Save settings", -1)])
				], 32)]),
				_: 1
			}, 8, ["open"])
		]));
	}
}), _m = { class: "tater-vue-surface tsx-spudex" }, vm = { class: "tv-page-heading" }, ym = { class: "tv-heading-actions" }, bm = { class: "tv-metrics" }, xm = {
	class: "tv-tabs tsx-tabs",
	"aria-label": "Spudex sections"
}, Sm = ["onClick"], Cm = { key: 0 }, wm = {
	key: 1,
	class: "tsx-workbench"
}, Tm = { class: "tv-panel tsx-session-bar" }, Em = { class: "tsx-session-actions" }, Dm = ["disabled"], Om = ["disabled"], km = { class: "tsx-session-list" }, Am = ["onClick"], jm = ["onClick"], Mm = {
	key: 0,
	class: "tv-empty compact"
}, Nm = { class: "tsx-workbench-grid" }, Pm = { class: "tv-panel tsx-console-card" }, Fm = { class: "tsx-console-head" }, Im = { class: "tsx-workbench-feed" }, Lm = {
	key: 0,
	class: "tsx-chat-feed"
}, Rm = {
	key: 1,
	class: "tsx-log-list"
}, zm = {
	key: 2,
	class: "tv-empty"
}, Bm = ["disabled"], Vm = ["disabled"], Hm = { class: "tv-panel tsx-processes" }, Um = ["onClick"], Wm = {
	key: 0,
	class: "tv-empty compact"
}, Gm = {
	key: 2,
	class: "tsx-manual"
}, Km = { class: "tv-panel tsx-run-card" }, qm = { class: "tv-state" }, Jm = { class: "tsx-check" }, Ym = ["disabled"], Xm = { class: "tv-panel tsx-manual-console" }, Zm = { class: "tsx-console-head" }, Qm = { class: "tsx-session-actions" }, $m = ["disabled"], eh = ["disabled"], th = { class: "tsx-manual-console-body" }, nh = {
	key: 0,
	class: "tv-empty"
}, rh = { class: "tv-panel tsx-manual-history" }, ih = ["onClick"], ah = {
	key: 0,
	class: "tv-empty compact"
}, oh = {
	key: 3,
	class: "tsx-settings"
}, sh = { class: "tv-panel tsx-access-card" }, ch = { class: "tsx-master-toggle" }, lh = { class: "tsx-settings-grid" }, uh = { class: "tsx-platforms" }, dh = ["checked", "onChange"], fh = { class: "tv-panel tsx-policy-card" }, ph = { class: "tsx-policy-grid" }, mh = ["onUpdate:modelValue"], hh = { class: "tsx-settings-save" }, gh = ["disabled"], _h = {
	class: "tv-modal tsx-details",
	role: "dialog",
	"aria-modal": "true",
	"aria-label": "Session details"
}, vh = {
	key: 0,
	class: "tsx-insights"
}, yh = {
	key: 0,
	class: "tsx-policy-notice danger"
}, bh = { key: 0 }, xh = {
	key: 0,
	class: "tsx-plan"
}, Sh = {
	key: 1,
	class: "tv-empty compact"
}, Ch = { key: 0 }, wh = {
	key: 1,
	class: "tv-empty compact"
}, Th = {
	key: 0,
	class: "tsx-preview-list"
}, Eh = ["href"], Dh = {
	key: 1,
	class: "tv-empty compact"
}, Oh = {
	key: 0,
	class: "tsx-git"
}, kh = { key: 0 }, Ah = {
	key: 1,
	class: "tv-empty compact"
}, jh = { class: "wide" }, Mh = {
	key: 0,
	class: "tsx-file-list"
}, Nh = { key: 0 }, Ph = ["onClick"], Fh = ["onClick"], Ih = {
	key: 1,
	class: "tv-empty compact"
}, Lh = { class: "wide" }, Rh = { key: 0 }, zh = {
	key: 1,
	class: "tv-empty compact"
}, Bh = {
	key: 1,
	class: "tv-empty"
}, Vh = /* @__PURE__ */ ar({
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
		], i = [
			[
				"require_approval",
				"Require Hydra approval",
				"Hydra-triggered actions pause for approval. Spudex Chat and manual commands remain direct."
			],
			[
				"require_file_approval",
				"Require file write approval",
				"Model-proposed file writes remain pending until approved or rejected."
			],
			[
				"allow_network",
				"Allow network commands",
				"Allows curl, wget, and Git network actions."
			],
			[
				"allow_installs",
				"Allow package and tool installs",
				"Allows pip, npm, uv, and similar environment installs."
			],
			[
				"allow_absolute_executables",
				"Allow absolute executable paths",
				"Allows commands such as /usr/bin/python3."
			],
			[
				"allow_shell_commands",
				"Allow shells",
				"Allows sh, bash, zsh, fish, cmd, and PowerShell."
			],
			[
				"allow_host_admin_commands",
				"Allow host and admin commands",
				"Allows sudo, chmod, chown, launchctl, osascript, and open."
			],
			[
				"allow_remote_control",
				"Allow remote control tools",
				"Allows ssh, scp, and sftp when network access is also enabled."
			],
			[
				"allow_containers",
				"Allow containers",
				"Allows Docker and Podman commands."
			],
			[
				"allow_host_package_managers",
				"Allow host package managers",
				"Allows brew, apt, yum, dnf, pacman, and apk."
			],
			[
				"allow_inline_eval",
				"Allow inline eval",
				"Allows python -c, node -e, ruby -e, and similar interpreter execution."
			]
		], a = /* @__PURE__ */ G(ae(n.options.initialTab)), o = /* @__PURE__ */ G(I(n.options.initialSessionId)), s = /* @__PURE__ */ G(I(n.options.initialManualSessionId)), c = /* @__PURE__ */ G([]), l = /* @__PURE__ */ G(0), u = /* @__PURE__ */ G([]), d = /* @__PURE__ */ G(0), f = /* @__PURE__ */ G(""), p = /* @__PURE__ */ G(""), m = /* @__PURE__ */ G(!1), h = /* @__PURE__ */ G(""), g = /* @__PURE__ */ G(""), _ = /* @__PURE__ */ G(""), v = /* @__PURE__ */ G(!1), y = /* @__PURE__ */ G(!1), b = /* @__PURE__ */ Ct({}), x = 0, S = !1, C = $(() => n.state.payload || {}), w = $(() => Array.isArray(C.value.sessions) ? C.value.sessions : []), T = $(() => w.value.filter((e) => L(e.source) === "ui")), E = $(() => Array.isArray(C.value.model_processes) ? C.value.model_processes : []), D = $(() => w.value.find((e) => I(e.id) === o.value) || null), O = $(() => T.value.find((e) => I(e.id) === s.value) || null), k = $(() => a.value === "manual" ? O.value : D.value), A = $(() => Number(C.value.active_count || w.value.filter(R).length)), ee = $(() => Number(C.value.model_process_count || E.value.length)), j = $(() => R(D.value)), M = $(() => R(O.value)), te = $(() => L(D.value?.source) === "spudex_chat" ? D.value : null), N = $(() => !!(h.value === "chat" || R(te.value))), ne = $(() => se(C.value.platform_options, b.allowed_platforms)), re = $(() => {
			let e = c.value.map((e) => {
				let t = L(e.stream), r = I(e.text);
				return r ? t === "user" ? {
					role: "user",
					username: n.options.profile?.username,
					content: r
				} : t === "assistant" || t === "system" && L(e.level) === "error" ? {
					role: "assistant",
					content: r
				} : null : null;
			}).filter(Boolean);
			return N.value ? [...e.slice(-20), {
				role: "assistant",
				content: { marker: "typing" }
			}] : e.slice(-20);
		}), P = $(() => c.value.filter((e) => !["user", "assistant"].includes(L(e.stream)))), F = $(() => {
			let e = te.value || D.value;
			if (h.value === "chat") return "Starting Spudex chat…";
			if (!e) return A.value ? `${A.value} active Spudex process${A.value === 1 ? "" : "es"}` : "Ready for a Spudex task.";
			let t = (Array.isArray(e.plan) ? e.plan : []).find((e) => L(e.status) === "in_progress");
			return t?.step ? `Working: ${t.step}` : `${B(e.status)}${I(e.label || e.command || e.goal) ? `: ${I(e.label || e.command || e.goal)}` : ""}`;
		});
		function I(e) {
			return String(e ?? "").trim();
		}
		function L(e) {
			return I(e).toLowerCase();
		}
		function ie(e) {
			return encodeURIComponent(I(e));
		}
		function ae(e) {
			let t = L(e);
			return t === "manual" || t === "settings" || t === "policy" ? t === "policy" ? "settings" : t : "workbench";
		}
		function R(e) {
			let t = L(e?.status);
			return !!e?.active || t === "running" || t === "queued";
		}
		function B(e) {
			let t = L(e) || "queued";
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
		function V(e) {
			let t = Number(e || 0);
			if (!t) return "";
			let n = Math.max(0, Math.floor(Date.now() / 1e3 - t));
			return n < 60 ? `${n}s ago` : n < 3600 ? `${Math.floor(n / 60)}m ago` : n < 86400 ? `${Math.floor(n / 3600)}h ago` : `${Math.floor(n / 86400)}d ago`;
		}
		function oe(e, t, n) {
			let r = n ? [] : [...e], i = new Set(r.map((e) => I(e.seq) || `${e.ts ?? ""}\u0000${e.stream ?? ""}\u0000${e.text ?? ""}`));
			return t.forEach((e) => {
				let t = I(e.seq) || `${e.ts ?? ""}\u0000${e.stream ?? ""}\u0000${e.text ?? ""}`;
				i.has(t) || (i.add(t), r.push(e));
			}), r.slice(-1e3);
		}
		function se(e, t) {
			let n = new Set((Array.isArray(t) ? t : ["webui"]).map(L).filter(Boolean)), r = /* @__PURE__ */ new Map();
			return (Array.isArray(e) ? e : []).forEach((e) => {
				let t = L(e.value);
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
		function H(e, t = "success") {
			_.value = e, g.value = t === "error" ? e : "", n.options.onToast?.(e, t);
		}
		function ce(e, t = "") {
			return `${n.options.endpoints.sessions}/${ie(e)}${t}`;
		}
		async function le(e) {
			return hs(await fetch(e, {
				method: "DELETE",
				credentials: "same-origin",
				headers: { Accept: "application/json" }
			}));
		}
		function ue(e = !1) {
			if (y.value && !e) return;
			let t = C.value.settings || {};
			Object.assign(b, {
				enabled: !!t.enabled,
				policy_enabled: t.policy_enabled !== !1,
				require_approval: !!t.require_approval,
				require_file_approval: !!t.require_file_approval,
				allow_absolute_executables: !!t.allow_absolute_executables,
				allow_shell_commands: !!t.allow_shell_commands,
				allow_host_admin_commands: !!t.allow_host_admin_commands,
				allow_remote_control: !!t.allow_remote_control,
				allow_containers: !!t.allow_containers,
				allow_host_package_managers: !!t.allow_host_package_managers,
				allow_inline_eval: !!t.allow_inline_eval,
				allow_network: !!t.allow_network,
				allow_installs: !!t.allow_installs,
				allowed_platforms: Array.isArray(t.allowed_platforms) ? [...t.allowed_platforms] : ["webui"],
				default_cwd: I(t.default_cwd || "workspace"),
				max_task_steps: Number(t.max_task_steps || 6),
				command_timeout_sec: Number(t.command_timeout_sec || 45)
			}), y.value = !1;
		}
		function de() {
			w.value.some((e) => I(e.id) === o.value) || pe(I(w.value[0]?.id), !1), T.value.some((e) => I(e.id) === s.value) || me(I(T.value[0]?.id), !1);
		}
		function fe(e) {
			a.value = ae(e), v.value = !1, n.options.onTabChange?.(a.value);
		}
		function pe(e, t = !0) {
			let r = I(e);
			r !== o.value && (o.value = r, c.value = [], l.value = 0, n.options.onSessionChange?.(r), t && ge(!0));
		}
		function me(e, t = !0) {
			let r = I(e);
			r !== s.value && (s.value = r, u.value = [], d.value = 0, n.options.onManualSessionChange?.(r), r && (o.value = r, n.options.onSessionChange?.(r)), t && _e(!0));
		}
		async function W(e = !1) {
			e || (h.value = "refresh");
			try {
				n.state.payload = await gs(n.options.endpoints.root), de(), ue();
			} catch (t) {
				e || H(t instanceof Error ? t.message : "Spudex refresh failed.", "error");
			} finally {
				!e && h.value === "refresh" && (h.value = "");
			}
		}
		async function he(e, t) {
			return gs(`${ce(e, "/logs")}?after_seq=${ie(t)}&limit=500`);
		}
		async function ge(e = !1) {
			let t = o.value;
			if (!t) {
				c.value = [], l.value = 0;
				return;
			}
			let n = await he(t, e ? 0 : l.value), r = Array.isArray(n.entries) ? n.entries : [];
			c.value = oe(c.value, r, e), l.value = Number(n.last_seq || (e ? 0 : l.value)), await sn();
			let i = document.querySelector(".tsx-workbench-feed");
			i instanceof HTMLElement && (e || i.scrollHeight - i.scrollTop - i.clientHeight < 120) && (i.scrollTop = i.scrollHeight);
		}
		async function _e(e = !1) {
			let t = s.value;
			if (!t) {
				u.value = [], d.value = 0;
				return;
			}
			let n = await he(t, e ? 0 : d.value), r = Array.isArray(n.entries) ? n.entries : [];
			u.value = oe(u.value, r, e), d.value = Number(n.last_seq || (e ? 0 : d.value)), await sn();
			let i = document.querySelector(".tsx-manual-console-body");
			i instanceof HTMLElement && (e || i.scrollHeight - i.scrollTop - i.clientHeight < 100) && (i.scrollTop = i.scrollHeight);
		}
		async function ve(e = !1) {
			await W(e), await Promise.all([ge(!1), _e(!1)]);
		}
		function ye() {
			x && window.clearTimeout(x), x = window.setTimeout(async () => {
				if (!S) {
					S = !0;
					try {
						await ve(!0);
					} catch {} finally {
						S = !1;
					}
				}
				ye();
			}, 2e3);
		}
		async function be() {
			let e = f.value.trim();
			if (!e) {
				H("Enter a Spudex chat message first.", "error");
				return;
			}
			if (N.value) {
				H("Spudex is still working in this chat.", "error");
				return;
			}
			h.value = "chat";
			try {
				let t = L(D.value?.source) === "spudex_chat" ? o.value : "", r = I((await _s(n.options.endpoints.chat, {
					message: e,
					session_id: t || null
				})).session?.id);
				r && pe(r, !1), f.value = "", H("Spudex task started."), await W(!0), await ge(!0);
			} catch (e) {
				H(e instanceof Error ? e.message : "Spudex chat failed.", "error");
			} finally {
				h.value = "";
			}
		}
		async function xe() {
			h.value = "new-chat";
			try {
				pe(I((await _s(n.options.endpoints.chatSession, { label: "New Spudex chat" })).session?.id), !1), f.value = "", H("New Spudex chat created."), await W(!0), await ge(!0);
			} catch (e) {
				H(e instanceof Error ? e.message : "New Spudex chat failed.", "error");
			} finally {
				h.value = "";
			}
		}
		async function Se() {
			let e = p.value.trim();
			if (!e) {
				H("Enter a command first.", "error");
				return;
			}
			h.value = "run";
			try {
				let t = I((await _s(n.options.endpoints.run, {
					command: e,
					label: e.slice(0, 80),
					background: m.value
				})).session?.id);
				pe(t, !1), me(t, !1), p.value = "", H("Spudex session started."), await W(!0), await Promise.all([ge(!0), _e(!0)]);
			} catch (e) {
				H(e instanceof Error ? e.message : "Command failed.", "error");
			} finally {
				h.value = "";
			}
		}
		async function Ce(e, t = "Spudex session") {
			if (e) {
				h.value = `stop-${e}`;
				try {
					await _s(ce(e, "/stop")), H(`${t} stop requested.`), await W(!0);
				} catch (e) {
					H(e instanceof Error ? e.message : "Stop failed.", "error");
				} finally {
					h.value = "";
				}
			}
		}
		async function we(e) {
			let t = I(e.id);
			if (t && !(R(e) && !window.confirm("Close this running Spudex session? Its active command will be stopped."))) {
				h.value = `close-${t}`;
				try {
					await le(ce(t)), t === o.value && pe("", !1), t === s.value && me("", !1), H("Spudex session closed."), await W(!0);
				} catch (e) {
					H(e instanceof Error ? e.message : "Close failed.", "error");
				} finally {
					h.value = "";
				}
			}
		}
		async function Te(e, t, n) {
			h.value = `${n}-${t}`;
			try {
				await _s(ce(e, `/file-changes/${n}`), { change_id: t }), H(`File change ${n === "approve" ? "approved" : "rejected"}.`), await W(!0);
			} catch (e) {
				H(e instanceof Error ? e.message : "File change update failed.", "error");
			} finally {
				h.value = "";
			}
		}
		async function Ee() {
			h.value = "settings";
			try {
				await _s(n.options.endpoints.settings, { values: {
					...b,
					allowed_platforms: b.allowed_platforms?.length ? b.allowed_platforms : ["webui"]
				} }), y.value = !1, H("Spudex settings saved."), await W(!0), ue(!0);
			} catch (e) {
				H(e instanceof Error ? e.message : "Spudex settings failed.", "error");
			} finally {
				h.value = "";
			}
		}
		function De(e, t) {
			let n = new Set((Array.isArray(b.allowed_platforms) ? b.allowed_platforms : []).map(L));
			t ? (e === "all" && n.clear(), n.add(e)) : n.delete(e), e !== "all" && t && n.delete("all"), b.allowed_platforms = [...n], y.value = !0;
		}
		function Oe(e) {
			e.key === "Enter" && !e.shiftKey && !e.ctrlKey && !e.altKey && !e.metaKey && !e.isComposing && (e.preventDefault(), be());
		}
		function ke(e) {
			e.key === "Escape" && (v.value = !1);
		}
		return En(() => n.state.payload, () => {
			de(), ue();
		}, { deep: !1 }), ue(!0), de(), window.addEventListener("keydown", ke), Promise.all([ge(!0), _e(!0)]).catch(() => {}), ye(), Cr(() => {
			x && window.clearTimeout(x), window.removeEventListener("keydown", ke);
		}), t({ refresh: () => ve(!1) }), (t, n) => (J(), Y(q, null, [X("div", _m, [
			X("header", vm, [n[24] ||= X("div", null, [
				X("span", { class: "tv-eyebrow" }, "Sandboxed agent workspace"),
				X("h1", null, "Spudex"),
				X("p", null, "Inspect, run, and guide local agent tasks inside Tater’s protected agent_lab workspace.")
			], -1), X("div", ym, [X("span", { class: z(["tv-live-pill", { busy: !!h.value }]) }, [n[23] ||= X("i", null, null, -1), Z(U(h.value ? "Working" : "Live"), 1)], 2), X("button", {
				class: "tv-button",
				type: "button",
				onClick: n[0] ||= (e) => ve(!1)
			}, "Refresh")])]),
			X("div", bm, [
				X("div", null, [n[25] ||= X("span", null, "Sessions", -1), X("strong", null, U(w.value.length), 1)]),
				X("div", null, [n[26] ||= X("span", null, "Active", -1), X("strong", null, U(A.value), 1)]),
				X("div", null, [n[27] ||= X("span", null, "Processes", -1), X("strong", null, U(ee.value), 1)]),
				X("div", null, [n[28] ||= X("span", null, "Policy", -1), X("strong", null, U(b.policy_enabled ? "On" : "Off"), 1)])
			]),
			_.value || g.value ? (J(), Y("div", {
				key: 0,
				class: z(["tv-notice", { error: !!g.value }])
			}, U(g.value || _.value), 3)) : Q("", !0),
			X("nav", xm, [(J(), Y(q, null, K(r, (e) => X("button", {
				key: e.id,
				type: "button",
				class: z({ active: a.value === e.id }),
				onClick: (t) => fe(e.id)
			}, [Z(U(e.label), 1), e.id === "workbench" && A.value ? (J(), Y("span", Cm, U(A.value), 1)) : Q("", !0)], 10, Sm)), 64))]),
			a.value === "workbench" ? (J(), Y("section", wm, [X("div", Tm, [X("header", null, [X("div", null, [
				n[29] ||= X("span", { class: "tv-eyebrow" }, "Sessions", -1),
				X("h2", null, U(D.value?.label || D.value?.command || "New chat"), 1),
				X("p", null, U(F.value), 1)
			]), X("div", Em, [
				X("button", {
					class: "tv-button",
					type: "button",
					disabled: !D.value,
					onClick: n[1] ||= (e) => v.value = !0
				}, "Details", 8, Dm),
				X("button", {
					class: "tv-button",
					type: "button",
					onClick: n[2] ||= (e) => c.value = []
				}, "Clear"),
				X("button", {
					class: "tv-button danger",
					type: "button",
					disabled: !j.value,
					onClick: n[3] ||= (e) => Ce(o.value)
				}, "Stop", 8, Om)
			])]), X("div", km, [(J(!0), Y(q, null, K(w.value, (e) => (J(), Y("article", {
				key: e.id,
				class: z({ selected: String(e.id) === o.value })
			}, [X("button", {
				type: "button",
				onClick: (t) => pe(String(e.id))
			}, [
				X("strong", null, U(e.label || e.command || "Spudex session"), 1),
				X("small", null, U(e.command || e.goal || (e.status === "draft" ? "Ready for first message" : "")), 1),
				X("span", null, [X("b", { class: z(["tv-state", { good: R(e) }]) }, U(B(e.status)), 3), X("time", null, U(V(e.updated_ts)), 1)])
			], 8, Am), X("button", {
				class: "tsx-session-close",
				type: "button",
				"aria-label": "Close Spudex session",
				title: "Close session",
				onClick: (t) => we(e)
			}, "×", 8, jm)], 2))), 128)), w.value.length ? Q("", !0) : (J(), Y("div", Mm, "No Spudex sessions yet."))])]), X("div", Nm, [X("section", Pm, [
				X("header", Fm, [X("div", null, [X("span", { class: z(["tsx-live-dot", { live: j.value }]) }, null, 2), X("div", null, [X("strong", null, U(D.value?.label || D.value?.command || "Spudex console"), 1), X("small", null, U(D.value ? `Session ${String(D.value.id).slice(0, 8)}` : "Start a new chat below"), 1)])]), X("span", null, U(A.value) + " active · " + U(ee.value) + " model process" + U(ee.value === 1 ? "" : "es"), 1)]),
				X("div", Im, [
					te.value && re.value.length ? (J(), Y("div", Lm, [(J(!0), Y(q, null, K(re.value, (t, n) => (J(), ea(qs, {
						key: `${n}-${t.role}`,
						message: t,
						profile: e.options.profile || {},
						"files-endpoint": e.options.endpoints.chatFiles
					}, null, 8, [
						"message",
						"profile",
						"files-endpoint"
					]))), 128))])) : Q("", !0),
					P.value.length ? (J(), Y("div", Rm, [(J(!0), Y(q, null, K(P.value, (e) => (J(), Y("article", {
						key: e.seq || `${e.ts}-${e.text}`,
						class: z(L(e.stream))
					}, [
						X("time", null, U(V(e.ts)), 1),
						X("span", null, U(e.stream || "log"), 1),
						X("pre", null, U(e.text), 1)
					], 2))), 128))])) : Q("", !0),
					!c.value.length && !N.value ? (J(), Y("div", zm, "Ask Spudex to inspect, run, or fix something inside agent_lab.")) : Q("", !0)
				]),
				X("form", {
					class: "tsx-composer",
					onSubmit: os(be, ["prevent"])
				}, [
					bn(X("textarea", {
						"onUpdate:modelValue": n[4] ||= (e) => f.value = e,
						rows: "1",
						placeholder: "Message Tater through Spudex…",
						disabled: N.value,
						onKeydown: Oe
					}, null, 40, Bm), [[qo, f.value]]),
					X("button", {
						class: "tv-button",
						type: "button",
						onClick: xe
					}, "New chat"),
					X("button", {
						class: "tv-button primary",
						type: "submit",
						disabled: N.value || !f.value.trim()
					}, U(N.value ? "Working…" : "Send"), 9, Vm),
					X("small", null, U(F.value), 1)
				], 32)
			]), X("aside", Hm, [
				X("header", null, [n[30] ||= X("div", null, [X("span", { class: "tv-eyebrow" }, "Tracked runtime"), X("h2", null, "Processes")], -1), X("span", { class: z(["tv-state", { good: E.value.length }]) }, U(E.value.length), 3)]),
				(J(!0), Y(q, null, K(E.value, (e) => (J(), Y("article", { key: e.session_id }, [X("div", null, [X("strong", null, U(e.label || e.command || "Spudex process"), 1), X("small", null, U([
					e.pid ? `PID ${e.pid}` : "PID pending",
					e.source,
					e.cwd
				].filter(Boolean).join(" · ")), 1)]), X("button", {
					class: "tv-button danger",
					type: "button",
					onClick: (t) => Ce(String(e.session_id), "Model process")
				}, "Kill", 8, Um)]))), 128)),
				E.value.length ? Q("", !0) : (J(), Y("div", Wm, "No model-launched processes running."))
			])])])) : a.value === "manual" ? (J(), Y("section", Gm, [
				X("div", Km, [X("header", null, [n[31] ||= X("div", null, [
					X("span", { class: "tv-eyebrow" }, "agent_lab"),
					X("h2", null, "Manual Session"),
					X("p", null, "Run one policy-controlled command from Tater’s working area.")
				], -1), X("span", qm, U(C.value.agent_lab || "agent_lab"), 1)]), X("form", { onSubmit: os(Se, ["prevent"]) }, [
					X("label", null, [n[32] ||= X("span", null, "Command", -1), bn(X("input", {
						"onUpdate:modelValue": n[5] ||= (e) => p.value = e,
						type: "text",
						autocomplete: "off",
						placeholder: "python --version"
					}, null, 512), [[qo, p.value]])]),
					X("label", Jm, [bn(X("input", {
						"onUpdate:modelValue": n[6] ||= (e) => m.value = e,
						class: "tv-checkbox",
						type: "checkbox"
					}, null, 512), [[Jo, m.value]]), n[33] ||= X("span", null, "Keep running", -1)]),
					X("button", {
						class: "tv-button primary",
						type: "submit",
						disabled: h.value === "run"
					}, "Run", 8, Ym)
				], 32)]),
				X("section", Xm, [X("header", Zm, [X("div", null, [n[34] ||= X("span", { class: "tsx-window-dots" }, [
					X("i"),
					X("i"),
					X("i")
				], -1), X("div", null, [X("strong", null, U(O.value?.command || O.value?.label || "Manual console"), 1), X("small", null, "tater@spudex:" + U(O.value?.cwd_display || "workspace"), 1)])]), X("div", Qm, [
					X("button", {
						class: "tv-button",
						type: "button",
						disabled: !O.value,
						onClick: n[7] ||= (e) => v.value = !0
					}, "Details", 8, $m),
					X("button", {
						class: "tv-button",
						type: "button",
						onClick: n[8] ||= (e) => u.value = []
					}, "Clear"),
					X("button", {
						class: "tv-button danger",
						type: "button",
						disabled: !M.value,
						onClick: n[9] ||= (e) => Ce(s.value, "Manual session")
					}, "Stop", 8, eh)
				])]), X("div", th, [(J(!0), Y(q, null, K(u.value, (e) => (J(), Y("article", {
					key: e.seq || `${e.ts}-${e.text}`,
					class: z(L(e.stream))
				}, [X("span", null, U(e.stream === "command" ? "$" : e.stream || "log"), 1), X("pre", null, U(String(e.text || "").replace(/^\$\s*/, "")), 1)], 2))), 128)), u.value.length ? Q("", !0) : (J(), Y("div", nh, U(s.value ? "Console is waiting for output." : "Run a command to open a manual console session."), 1))])]),
				X("div", rh, [n[35] ||= X("header", null, [X("div", null, [X("span", { class: "tv-eyebrow" }, "Recent commands"), X("h2", null, "Manual History")])], -1), X("div", null, [(J(!0), Y(q, null, K(T.value.slice(0, 10), (e) => (J(), Y("button", {
					key: e.id,
					type: "button",
					class: z({ selected: String(e.id) === s.value }),
					onClick: (t) => me(String(e.id))
				}, [X("span", null, [X("strong", null, U(e.command || e.label || "Manual run"), 1), X("small", null, U(V(e.updated_ts)), 1)]), X("b", { class: z(["tv-state", { good: R(e) }]) }, U(B(e.status)), 3)], 10, ih))), 128)), T.value.length ? Q("", !0) : (J(), Y("div", ah, "No manual runs yet."))])])
			])) : (J(), Y("section", oh, [
				X("div", sh, [
					X("header", null, [n[36] ||= X("div", null, [
						X("span", { class: "tv-eyebrow" }, "Hydra access"),
						X("h2", null, "Spudex availability"),
						X("p", null, "Expose policy-controlled Spudex tools only on the Tater surfaces you choose.")
					], -1), X("label", ch, [X("span", null, U(b.enabled ? "Enabled" : "Off"), 1), bn(X("input", {
						"onUpdate:modelValue": n[10] ||= (e) => b.enabled = e,
						class: "tv-checkbox",
						type: "checkbox",
						onChange: n[11] ||= (e) => y.value = !0
					}, null, 544), [[Jo, b.enabled]])])]),
					X("div", lh, [
						X("label", null, [n[37] ||= X("span", null, "Default working folder", -1), bn(X("input", {
							"onUpdate:modelValue": n[12] ||= (e) => b.default_cwd = e,
							type: "text",
							onInput: n[13] ||= (e) => y.value = !0
						}, null, 544), [[qo, b.default_cwd]])]),
						X("label", null, [n[38] ||= X("span", null, "Max task steps", -1), bn(X("input", {
							"onUpdate:modelValue": n[14] ||= (e) => b.max_task_steps = e,
							type: "number",
							min: "1",
							max: "50",
							onInput: n[15] ||= (e) => y.value = !0
						}, null, 544), [[
							qo,
							b.max_task_steps,
							void 0,
							{ number: !0 }
						]])]),
						X("label", null, [n[39] ||= X("span", null, "Command timeout (seconds)", -1), bn(X("input", {
							"onUpdate:modelValue": n[16] ||= (e) => b.command_timeout_sec = e,
							type: "number",
							min: "5",
							max: "3600",
							onInput: n[17] ||= (e) => y.value = !0
						}, null, 544), [[
							qo,
							b.command_timeout_sec,
							void 0,
							{ number: !0 }
						]])])
					]),
					X("div", uh, [n[40] ||= X("div", null, [X("strong", null, "Platforms"), X("small", null, "Select where Hydra can expose Spudex.")], -1), (J(!0), Y(q, null, K(ne.value, (e) => (J(), Y("label", {
						key: e.value,
						class: z({ running: e.running })
					}, [X("span", null, [X("strong", null, U(e.label || e.value), 1), X("small", null, U(e.value === "all" ? "Every platform" : e.running ? "Running" : "Stopped") + " · " + U(e.description || "Available platform"), 1)]), X("input", {
						class: "tv-checkbox",
						type: "checkbox",
						checked: b.allowed_platforms?.includes(e.value),
						onChange: (t) => De(String(e.value), t.target.checked)
					}, null, 40, dh)], 2))), 128))])
				]),
				X("div", fh, [
					X("header", null, [n[41] ||= X("div", null, [
						X("span", { class: "tv-eyebrow" }, "Defense in depth"),
						X("h2", null, "Spudex policy"),
						X("p", null, "Keep command safety on, then allow only the categories a workflow actually needs.")
					], -1), X("label", { class: z(["tsx-master-toggle", { danger: !b.policy_enabled }]) }, [X("span", null, U(b.policy_enabled ? "Policy on" : "Policy off"), 1), bn(X("input", {
						"onUpdate:modelValue": n[18] ||= (e) => b.policy_enabled = e,
						class: "tv-checkbox",
						type: "checkbox",
						onChange: n[19] ||= (e) => y.value = !0
					}, null, 544), [[Jo, b.policy_enabled]])], 2)]),
					X("div", { class: z(["tsx-policy-notice", { danger: !b.policy_enabled }]) }, [X("strong", null, U(b.policy_enabled ? "Policy is active." : "Command safety policy is off."), 1), Z(" " + U(b.policy_enabled ? "Tater checks commands, paths, network use, installs, and the configurable categories below." : "Spudex can use shells, host paths, network commands, installs, and host-affecting tools."), 1)], 2),
					n[42] ||= X("div", { class: "tsx-guardrails" }, [
						X("span", null, [
							Z("Commands start inside "),
							X("code", null, "agent_lab"),
							Z(".")
						]),
						X("span", null, [
							Z("File writes stay inside "),
							X("code", null, "agent_lab"),
							Z(".")
						]),
						X("span", null, "Model processes stay tracked and stoppable.")
					], -1),
					X("div", ph, [(J(), Y(q, null, K(i, (e) => X("label", { key: e[0] }, [X("span", null, [X("strong", null, U(e[1]), 1), X("small", null, U(e[2]), 1)]), bn(X("input", {
						"onUpdate:modelValue": (t) => b[e[0]] = t,
						class: "tv-checkbox",
						type: "checkbox",
						onChange: n[20] ||= (e) => y.value = !0
					}, null, 40, mh), [[Jo, b[e[0]]]])])), 64))])
				]),
				X("div", hh, [n[43] ||= X("span", null, "Model routing remains in Settings → Models.", -1), X("button", {
					class: "tv-button primary",
					type: "button",
					disabled: h.value === "settings" || !y.value,
					onClick: Ee
				}, U(h.value === "settings" ? "Saving…" : "Save settings"), 9, gh)])
			]))
		]), aa(sl, {
			open: v.value,
			onClose: n[22] ||= (e) => v.value = !1
		}, {
			default: yn(() => [X("section", _h, [X("header", null, [X("div", null, [n[44] ||= X("span", { class: "tv-eyebrow" }, "Session details", -1), X("h2", null, U(k.value?.label || k.value?.command || "No session selected"), 1)]), X("button", {
				class: "tv-button",
				type: "button",
				onClick: n[21] ||= (e) => v.value = !1
			}, "Close")]), k.value ? (J(), Y("div", vh, [
				k.value.last_policy_block ? (J(), Y("div", yh, [
					X("strong", null, U(k.value.last_policy_block.title || "Command blocked"), 1),
					Z(" " + U(k.value.last_policy_block.reason || k.value.last_policy_block.message), 1),
					k.value.last_policy_block.toggle ? (J(), Y("small", bh, "Policy toggle: " + U(k.value.last_policy_block.toggle), 1)) : Q("", !0)
				])) : Q("", !0),
				X("article", null, [n[45] ||= X("h3", null, "Plan", -1), k.value.plan?.length ? (J(), Y("ol", xh, [(J(!0), Y(q, null, K(k.value.plan, (e) => (J(), Y("li", {
					key: e.step,
					class: z(L(e.status))
				}, [X("span", null, U(e.step || "Step"), 1), X("small", null, [Z(U(String(e.status || "pending").replaceAll("_", " ")), 1), e.detail ? (J(), Y(q, { key: 0 }, [Z(" · " + U(e.detail), 1)], 64)) : Q("", !0)])], 2))), 128))])) : (J(), Y("div", Sh, "No task plan yet."))]),
				X("article", null, [n[46] ||= X("h3", null, "Verification", -1), k.value.verification ? (J(), Y("div", {
					key: 0,
					class: z(["tsx-verification", L(k.value.verification.status)])
				}, [
					X("strong", null, U(k.value.verification.status === "passed" ? "Verification passed" : k.value.verification.status === "failed" ? "Verification failed" : "Verification recorded"), 1),
					X("small", null, U(k.value.verification.command), 1),
					k.value.verification.summary ? (J(), Y("pre", Ch, U(k.value.verification.summary), 1)) : Q("", !0)
				], 2)) : (J(), Y("div", wh, "No verification run yet."))]),
				X("article", null, [n[47] ||= X("h3", null, "App previews", -1), k.value.previews?.length ? (J(), Y("div", Th, [(J(!0), Y(q, null, K(k.value.previews.slice(-6).reverse(), (e) => (J(), Y("a", {
					key: e.url,
					href: e.url,
					target: "_blank",
					rel: "noreferrer"
				}, [X("span", null, U(e.url), 1), X("small", null, U(e.source || "preview"), 1)], 8, Eh))), 128))])) : (J(), Y("div", Dh, "No app previews detected yet."))]),
				X("article", null, [n[48] ||= X("h3", null, "Git", -1), C.value.git?.ok ? (J(), Y("div", Oh, [
					X("div", null, [X("strong", null, U(C.value.git.branch || "detached"), 1), X("small", null, U(C.value.git.repo), 1)]),
					X("span", { class: z(["tv-state", { good: !C.value.git.dirty }]) }, U(C.value.git.dirty ? `${C.value.git.changed_count || C.value.git.changed_files?.length || 0} changed` : "Clean"), 3),
					C.value.git.changed_files?.length ? (J(), Y("pre", kh, U(C.value.git.changed_files.slice(0, 24).join("\n")), 1)) : Q("", !0)
				])) : (J(), Y("div", Ah, "No Git repository detected."))]),
				X("article", jh, [n[49] ||= X("h3", null, "File changes", -1), k.value.file_changes?.length ? (J(), Y("div", Mh, [(J(!0), Y(q, null, K(k.value.file_changes.slice(-6).reverse(), (e) => (J(), Y("section", {
					key: e.id,
					class: z({
						pending: e.pending,
						applied: e.applied
					})
				}, [X("header", null, [X("div", null, [X("strong", null, U(e.path_display || e.path || "File change"), 1), X("small", null, [Z(U(e.pending ? "Pending" : e.applied ? "Applied" : "Rejected"), 1), e.bytes ? (J(), Y(q, { key: 0 }, [Z(" · " + U(e.bytes) + " bytes", 1)], 64)) : Q("", !0)])]), e.pending ? (J(), Y("div", Nh, [X("button", {
					class: "tv-button",
					type: "button",
					onClick: (t) => Te(String(k.value.id), String(e.id), "approve")
				}, "Approve", 8, Ph), X("button", {
					class: "tv-button danger",
					type: "button",
					onClick: (t) => Te(String(k.value.id), String(e.id), "reject")
				}, "Reject", 8, Fh)])) : Q("", !0)]), X("pre", null, U(e.diff || "No textual diff available."), 1)], 2))), 128))])) : (J(), Y("div", Ih, "No file changes yet."))]),
				X("article", Lh, [n[50] ||= X("h3", null, "Session memory", -1), k.value.memory_summary ? (J(), Y("p", Rh, U(k.value.memory_summary), 1)) : (J(), Y("div", zh, "No session memory yet."))])
			])) : (J(), Y("div", Bh, "Select a session to see its details."))])]),
			_: 1
		}, 8, ["open"])], 64));
	}
}), Hh = { class: "tater-vue-surface tset-settings" }, Uh = { class: "tv-page-heading" }, Wh = { class: "tv-heading-actions" }, Gh = { class: "tv-metrics tset-metrics" }, Kh = {
	class: "tv-tabs tset-tabs",
	"aria-label": "Settings sections"
}, qh = ["data-settings-vue-tab", "onClick"], Jh = {
	class: "tset-context",
	"aria-live": "polite"
}, Yh = /* @__PURE__ */ ar({
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
				description: "Identity, login, avatars, and everyday WebUI behavior."
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
				label: "Voice",
				description: "Satellites, firmware, stereo pairs, voice processing, and live controls."
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
				description: "Background snapshots, scheduled maintenance, run history, and manual refresh controls."
			},
			{
				id: "logs",
				label: "Logs",
				description: "Live application logs with filters, pause, copy, and tail controls."
			}
		], i = new Set(r.map((e) => e.id)), a = (e) => {
			let t = String(e || "").trim().toLowerCase();
			return i.has(t) ? t : "general";
		}, o = /* @__PURE__ */ G(a(n.options.initialTab)), s = $(() => r.find((e) => e.id === o.value) || r[0]), c = $(() => n.state.summary || {});
		function l(e, t = !1) {
			let r = a(e);
			o.value = r, t && n.options.onTabChange?.(r);
		}
		return t({ select: (e) => l(e, !1) }), (e, t) => (J(), Y("div", Hh, [
			X("header", Uh, [t[1] ||= X("div", null, [
				X("span", { class: "tv-eyebrow" }, "Tater configuration"),
				X("h1", null, "Settings"),
				X("p", null, "Configure identity, intelligence, voice, storage, security, and diagnostics from one workspace.")
			], -1), X("div", Wh, [X("span", { class: z(["tv-live-pill", { warning: !c.value.redisConnected }]) }, [t[0] ||= X("i", null, null, -1), Z(U(c.value.redisConnected ? "Services connected" : "Redis needs attention"), 1)], 2)])]),
			X("div", Gh, [
				X("div", null, [t[2] ||= X("span", null, "Redis", -1), X("strong", null, U(c.value.redisConnected ? "Connected" : "Setup needed"), 1)]),
				X("div", null, [t[3] ||= X("span", null, "Admin gated", -1), X("strong", null, U(Number(c.value.adminGateCount || 0)), 1)]),
				X("div", null, [t[4] ||= X("span", null, "Integrations", -1), X("strong", null, U(Number(c.value.integrationCount || 0)), 1)])
			]),
			X("nav", Kh, [(J(), Y(q, null, K(r, (e) => X("button", {
				key: e.id,
				type: "button",
				class: z({ active: o.value === e.id }),
				"data-settings-vue-tab": e.id,
				onClick: (t) => l(e.id, !0)
			}, U(e.label), 11, qh)), 64))]),
			X("div", Jh, [X("span", null, U(s.value.label), 1), X("p", null, U(s.value.description), 1)])
		]));
	}
}), Xh = { class: "tr-pill-main" }, Zh = {
	key: 0,
	class: "tr-pill-metrics"
}, Qh = { class: "tr-pill-models" }, $h = { class: "tr-pill-resources" }, eg = {
	class: "tv-modal tr-modal",
	role: "dialog",
	"aria-modal": "true",
	"aria-label": "Runtime statistics"
}, tg = { class: "tr-modal-head" }, ng = { class: "tr-modal-actions" }, rg = ["disabled"], ig = {
	key: 1,
	class: "tv-empty"
}, ag = {
	key: 2,
	class: "tr-grid"
}, og = { class: "tv-panel tr-card wide models" }, sg = { class: "tr-meter-grid" }, cg = { class: "tr-meter-track" }, lg = { key: 0 }, ug = {
	key: 0,
	class: "tr-block"
}, dg = { class: "tr-list" }, fg = { class: "tv-state good" }, pg = { class: "tr-block" }, mg = {
	key: 0,
	class: "tr-list"
}, hg = { key: 0 }, gg = {
	key: 1,
	class: "danger"
}, _g = ["disabled", "onClick"], vg = {
	key: 1,
	class: "tv-state good"
}, yg = {
	key: 1,
	class: "tv-empty compact"
}, bg = { class: "tv-panel tr-card wide hydra" }, xg = { class: "tr-block" }, Sg = {
	key: 0,
	class: "tr-turns"
}, Cg = { class: "tv-state good" }, wg = { key: 0 }, Tg = { key: 1 }, Eg = { key: 0 }, Dg = { key: 1 }, Og = {
	key: 1,
	class: "tv-empty compact"
}, kg = { class: "tv-panel tr-card calls" }, Ag = { class: "tr-block" }, jg = {
	key: 0,
	class: "tr-list"
}, Mg = { class: "tv-state good" }, Ng = {
	key: 1,
	class: "tv-empty compact"
}, Pg = { class: "tv-panel tr-card vision" }, Fg = { class: "tr-block" }, Ig = {
	key: 0,
	class: "tr-list"
}, Lg = { class: "tv-state good" }, Rg = {
	key: 1,
	class: "tv-empty compact"
}, zg = { class: "tv-panel tr-card wide context" }, Bg = { key: 0 }, Vg = { key: 1 }, Hg = { key: 2 }, Ug = {
	key: 0,
	class: "tr-block"
}, Wg = { class: "tr-list dense" }, Gg = { class: "tv-state" }, Kg = /* @__PURE__ */ ar({
	__name: "RuntimeStatus",
	props: {
		state: {},
		options: {}
	},
	setup(e, { expose: t }) {
		let n = e, r = /* @__PURE__ */ G(!1), i = /* @__PURE__ */ G(!1), a = /* @__PURE__ */ G(""), o = /* @__PURE__ */ G(""), s = /* @__PURE__ */ G(""), c = /* @__PURE__ */ G({}), l = /* @__PURE__ */ G(null), u = /* @__PURE__ */ G(""), d = 0, f = $(() => n.state.health || {}), p = $(() => L(f.value.loaded_models || f.value.loadedModels)), m = $(() => L(p.value.system)), h = $(() => L(m.value.cpu)), g = $(() => L(m.value.ram)), _ = $(() => L(m.value.vram)), v = $(() => V(p.value.loaded_count)), y = $(() => n.state.text || `${V(f.value.verbas_enabled)} verba enabled • ${V(f.value.portals_running)} portals running • ${V(f.value.cores_running)} cores running • ${V(f.value.hydra_jobs_active ?? f.value.chat_jobs_active)} hydra jobs • ${V(f.value.llm_calls_active)} llm calls • ${V(f.value.vision_calls_active ?? f.value.voice_calls_active)} vision calls`), b = $(() => {
			let e = V(L(p.value.totals).estimated_total_bytes);
			return `${v.value} model${v.value === 1 ? "" : "s"} loaded${e > 0 ? ` • est ${ce(e)}` : ""}`;
		}), x = $(() => {
			let e = V(g.value.total_bytes), t = V(g.value.used_bytes), n = V(_.value.total_bytes), r = V(_.value.used_bytes), i = se(_.value.utilization_percent), a = !!(m.value.unified_memory || _.value.unified), o = [
				le("CPU", h.value.percent, h.value.available === !1),
				le("GPU", i, i === null),
				le(a ? "Unified" : "RAM", e > 0 ? g.value.percent ?? t / e * 100 : null, e <= 0)
			];
			return a || o.push(le("VRAM", n > 0 ? _.value.percent ?? r / n * 100 : null, n <= 0)), o;
		}), S = $(() => L(c.value.hydra_jobs || c.value.chat_jobs)), C = $(() => L(c.value.llm_calls)), w = $(() => L(c.value.vision_calls || c.value.voice_calls)), T = $(() => L(c.value.chat_context_window)), E = $(() => L(c.value.loaded_models)), D = $(() => L(E.value.system)), O = $(() => L(D.value.cpu)), k = $(() => L(D.value.ram)), A = $(() => L(D.value.vram)), ee = $(() => ie(E.value.models)), j = $(() => ie(A.value.devices)), M = $(() => ie(S.value.active_turns)), te = $(() => ie(C.value.active_calls)), N = $(() => ie(w.value.active_calls)), ne = $(() => {
			let e = L(E.value.totals);
			return [
				`${V(E.value.loaded_count ?? ee.value.length)} loaded`,
				V(E.value.local_llm_loaded_count) ? `${V(E.value.local_llm_loaded_count)} LLM` : "",
				V(E.value.managed_loaded_count) ? `${V(E.value.managed_loaded_count)} managed voice` : "",
				V(e.estimated_total_bytes) ? `est ${ce(e.estimated_total_bytes)}` : "",
				V(e.estimated_vram_bytes) ? `VRAM est ${ce(e.estimated_vram_bytes)}` : "",
				V(e.estimated_ram_bytes) ? `RAM est ${ce(e.estimated_ram_bytes)}` : "",
				V(e.estimated_unified_bytes) ? `unified est ${ce(e.estimated_unified_bytes)}` : ""
			].filter(Boolean).join(" • ") || "No loaded runtime models";
		}), re = $(() => {
			let e = L(T.value.breakdown), t = V(T.value.history_messages), n = V(T.value.max_history_messages) || t, r = [
				["System prompt", e.system_tokens],
				["Runtime status", e.status_tokens],
				["Core context + preamble", V(e.core_context_tokens) + V(e.platform_preamble_tokens)],
				[`Chat history (${t}/${n} msgs)`, e.history_tokens],
				["Current user turn", e.user_tokens]
			], i = V(T.value.capability_context_reserve_tokens ?? e.capability_reserve_tokens);
			return i && r.push(["Capability reserve", i]), r.map(([e, t]) => ({
				label: String(e),
				tokens: V(t)
			}));
		}), P = $(() => [
			`Prompt ${oe(T.value.prompt_tokens)} tok`,
			`Reply budget ${oe(T.value.completion_budget_tokens)} tok`,
			V(T.value.capability_context_reserve_tokens) ? `Capability reserve ${oe(T.value.capability_context_reserve_tokens)} tok` : "",
			V(T.value.burst_context_reserve_tokens) ? `Burst reserve ${oe(T.value.burst_context_reserve_tokens)} tok` : "",
			`Min window ${oe(T.value.minimum_context_window)}`,
			`Recommended ${oe(T.value.recommended_context_window)}`
		].filter(Boolean).join(" • ")), F = $(() => {
			let e = L(T.value.breakdown), t = ae(e.high_context_verba_examples).slice(0, 4);
			return [V(T.value.burst_context_reserve_tokens) ? `Recommended window includes ${oe(T.value.burst_context_reserve_tokens)} tokens of burst reserve for heavy or multi-tool turns.` : "", V(e.high_context_verbas) || V(e.heavy_cores) ? `High-context signals: ${V(e.high_context_verbas)} high-context verbas • ${V(e.heavy_cores)} heavy cores${t.length ? ` • e.g. ${t.join(", ")}` : ""}` : ""].filter(Boolean);
		});
		function L(e) {
			return e && typeof e == "object" && !Array.isArray(e) ? e : {};
		}
		function ie(e) {
			return Array.isArray(e) ? e.filter((e) => e && typeof e == "object") : [];
		}
		function ae(e) {
			return Array.isArray(e) ? e.map((e) => B(e)).filter(Boolean) : [];
		}
		function R(e) {
			return Array.isArray(e) ? e.map((e) => Number(e)).filter(Number.isFinite) : [];
		}
		function B(e) {
			return String(e ?? "").trim();
		}
		function V(e) {
			let t = Number(e);
			return Number.isFinite(t) ? Math.max(0, t) : 0;
		}
		function oe(e) {
			return Math.round(V(e)).toLocaleString();
		}
		function se(e) {
			if (e == null || B(e) === "") return null;
			let t = Number(e);
			return Number.isFinite(t) && t >= 0 ? Math.max(0, Math.min(100, t)) : null;
		}
		function H(e) {
			let t = se(e);
			return t === null ? "n/a" : `${Math.round(t)}%`;
		}
		function ce(e) {
			let t = V(e);
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
		function le(e, t, n) {
			let r = se(t);
			return {
				label: e,
				percent: r ?? 0,
				value: r === null ? "n/a" : `${Math.round(r)}%`,
				unavailable: n
			};
		}
		function ue(e) {
			let t = Math.round(V(e));
			if (t < 60) return `${t}s`;
			let n = Math.floor(t / 60);
			return n < 60 ? `${n}m ${t % 60}s` : `${Math.floor(n / 60)}h ${n % 60}m`;
		}
		function de(e) {
			let t = V(e);
			return t ? `Loaded ${(/* @__PURE__ */ new Date(t * 1e3)).toLocaleTimeString([], {
				hour: "numeric",
				minute: "2-digit"
			})}` : "";
		}
		function fe(e) {
			return [
				B(e.kind_label || e.category),
				B(e.provider_label || e.provider || "Local"),
				B(e.device) ? `Device ${B(e.device)}` : "",
				V(e.estimated_bytes) ? `${B(e.memory_kind || "ram").toUpperCase()} est ${ce(e.estimated_bytes)}` : "Estimate unavailable",
				de(e.loaded_ts)
			].filter(Boolean).join(" • ");
		}
		function pe(e) {
			return [...ae(e.details), e.managed ? B(e.managed_by || "Managed by settings") : ""].filter(Boolean);
		}
		function me(e) {
			let t = Number(e.power_draw_w), n = Number(e.power_limit_w);
			return [
				se(e.utilization_percent) === null ? "GPU load n/a" : `GPU ${H(e.utilization_percent)}`,
				V(e.total_bytes) ? `${e.unified ? "GPU memory" : "VRAM"} ${ce(e.used_bytes)} / ${ce(e.total_bytes)}` : "",
				V(e.shared_memory_total_bytes) ? `Shared RAM ${ce(e.shared_memory_used_bytes)} / ${ce(e.shared_memory_total_bytes)}` : "",
				Number.isFinite(Number(e.temperature_c)) ? `${Number(e.temperature_c).toFixed(0)} C` : "",
				Number.isFinite(t) ? `${t.toFixed(0)} W${Number.isFinite(n) && n > 0 ? ` / ${n.toFixed(0)} W` : ""}` : "",
				B(e.detail)
			].filter(Boolean).join(" • ");
		}
		function W(e, t) {
			return [
				`Model ${B(e.model || "model")}`,
				B(t === "llm" ? e.host : e.api_base),
				B(e.activity) ? `Activity ${B(e.activity)}` : B(e.function) ? `Fn ${B(e.function)}` : "",
				V(e.message_count) ? `${V(e.message_count)} msgs` : ""
			].filter(Boolean).join(" • ");
		}
		function he(e, t, n, r, i = "") {
			let a = V(n), o = V(t), s = se(r) ?? (a > 0 ? Math.max(0, Math.min(100, o / a * 100)) : null);
			return {
				label: e,
				percent: s ?? 0,
				value: r === void 0 ? a > 0 ? `${ce(o)} / ${ce(a)}` : "Unavailable" : H(r),
				unavailable: s === null,
				detail: i
			};
		}
		let ge = $(() => {
			let e = !!(D.value.unified_memory || A.value.unified), t = [
				he("CPU Usage", 0, 0, O.value.percent, [
					V(O.value.logical_count) ? `${V(O.value.logical_count)} logical cores` : "",
					V(O.value.physical_count) ? `${V(O.value.physical_count)} physical cores` : "",
					R(O.value.load_average).length ? `load ${R(O.value.load_average).map((e) => e.toFixed(2)).join(" / ")}` : ""
				].filter(Boolean).join(" • ")),
				he("GPU Usage", 0, 0, A.value.utilization_percent, [
					B(A.value.backend) ? `Backend ${B(A.value.backend)}` : "",
					j.value.length ? `${j.value.length} device${j.value.length === 1 ? "" : "s"}` : "",
					e ? "shared/unified memory" : "",
					se(A.value.utilization_percent) === null ? "GPU load unavailable from this runtime" : ""
				].filter(Boolean).join(" • ")),
				he(e ? "Unified Memory" : "System RAM", k.value.used_bytes, k.value.total_bytes)
			];
			return e || t.push(he("System VRAM", A.value.used_bytes, A.value.total_bytes)), t;
		});
		async function _e(e = !1) {
			if (!i.value) {
				i.value = !0, a.value = "", !e && !Object.keys(c.value).length && (o.value = "Loading runtime state…");
				try {
					let t = n.options.endpoints.breakdown, r = await gs(e ? t : `${t}${t.includes("?") ? "&" : "?"}refresh=true`);
					c.value = r || {}, s.value = (/* @__PURE__ */ new Date()).toLocaleTimeString(), o.value = "", n.options.onBreakdownChange?.(r || {});
				} catch (e) {
					a.value = e instanceof Error ? e.message : "Runtime breakdown failed.";
				} finally {
					i.value = !1;
				}
			}
		}
		function ve() {
			ye(), d = window.setInterval(() => {
				r.value && _e(!0);
			}, 5e3);
		}
		function ye() {
			d && window.clearInterval(d), d = 0;
		}
		async function be() {
			r.value = !0, await sn(), l.value?.focus(), await _e(!1), ve();
		}
		function xe() {
			r.value = !1, ye();
		}
		async function Se(e) {
			let t = B(e.cache_key || e.model);
			if (!(!t || u.value)) {
				u.value = t;
				try {
					let t = V((await _s(n.options.endpoints.unloadModel, {
						provider: B(e.provider),
						model: B(e.model),
						cache_key: B(e.cache_key)
					})).unloaded_count), r = t ? `Unloaded ${t} local model${t === 1 ? "" : "s"}.` : "No loaded model matched.";
					n.options.onToast?.(r, "success"), await _e(!0), n.options.onHealthRefresh?.();
				} catch (e) {
					let t = e instanceof Error ? e.message : "Model unload failed.";
					a.value = `Unload failed: ${t}`, n.options.onToast?.(a.value, "error");
				} finally {
					u.value = "";
				}
			}
		}
		function Ce(e) {
			e.key === "Escape" && r.value && xe();
		}
		return window.addEventListener("keydown", Ce), Cr(() => {
			ye(), window.removeEventListener("keydown", Ce);
		}), t({ open: be }), (t, n) => (J(), Y(q, null, [X("button", {
			class: z(["tr-pill", e.state.tone]),
			type: "button",
			title: "Open loaded models, CPU/GPU usage, memory, Hydra jobs, LLM calls, and vision calls",
			onClick: be
		}, [X("span", Xh, [n[1] ||= X("i", null, null, -1), Z(U(y.value), 1)]), e.state.health ? (J(), Y("span", Zh, [X("span", Qh, U(b.value), 1), X("span", $h, [(J(!0), Y(q, null, K(x.value, (e) => (J(), Y("span", {
			key: e.label,
			class: z(["tr-resource", { unavailable: e.unavailable }])
		}, [
			X("b", null, U(e.label), 1),
			X("span", null, [X("i", { style: I({ width: `${e.percent}%` }) }, null, 4)]),
			X("em", null, U(e.value), 1)
		], 2))), 128))])])) : Q("", !0)], 2), aa(sl, {
			open: r.value,
			"backdrop-class": "tv-modal-backdrop tr-backdrop",
			onClose: xe
		}, {
			default: yn(() => [X("section", eg, [
				X("header", tg, [
					n[2] ||= X("span", { class: "tr-modal-badge" }, "RT", -1),
					n[3] ||= X("div", null, [
						X("span", { class: "tv-eyebrow" }, "Runtime stats"),
						X("h2", null, "Live Activity"),
						X("p", null, "Loaded models, compute and memory usage, Hydra turns, model calls, vision work, and context budget.")
					], -1),
					X("div", ng, [
						X("span", null, U(s.value ? `Updated ${s.value}` : "Live data"), 1),
						X("button", {
							class: "tv-button",
							type: "button",
							disabled: i.value,
							onClick: n[0] ||= (e) => _e(!1)
						}, U(i.value ? "Refreshing…" : "Refresh"), 9, rg),
						X("button", {
							ref_key: "closeButton",
							ref: l,
							class: "tv-button",
							type: "button",
							onClick: xe
						}, "Close", 512)
					])
				]),
				a.value || o.value ? (J(), Y("div", {
					key: 0,
					class: z(["tv-notice", { error: !!a.value }])
				}, U(a.value || o.value), 3)) : Q("", !0),
				!Object.keys(c.value).length && i.value ? (J(), Y("div", ig, "Loading runtime state…")) : (J(), Y("div", ag, [
					X("article", og, [
						X("header", null, [X("div", null, [
							n[4] ||= X("span", { class: "tv-eyebrow" }, "Compute and memory", -1),
							n[5] ||= X("h2", null, "Loaded Runtime Models", -1),
							X("p", null, U(ne.value), 1)
						])]),
						X("div", sg, [(J(!0), Y(q, null, K(ge.value, (e) => (J(), Y("div", {
							key: e.label,
							class: z(["tr-meter", { unavailable: e.unavailable }])
						}, [
							X("div", null, [X("strong", null, U(e.label), 1), X("span", null, U(e.value), 1)]),
							X("span", cg, [X("i", { style: I({ width: `${e.percent}%` }) }, null, 4)]),
							e.detail ? (J(), Y("small", lg, U(e.detail), 1)) : Q("", !0)
						], 2))), 128))]),
						j.value.length ? (J(), Y("section", ug, [n[6] ||= X("h3", null, "GPU Devices", -1), X("div", dg, [(J(!0), Y(q, null, K(j.value, (e, t) => (J(), Y("article", { key: e.index ?? t }, [X("div", null, [X("strong", null, U(e.name || `GPU ${e.index ?? ""}`), 1), X("small", null, U(me(e)), 1)]), X("span", fg, U(H(e.utilization_percent)), 1)]))), 128))])])) : Q("", !0),
						X("section", pg, [n[7] ||= X("h3", null, "Loaded Model Entries", -1), ee.value.length ? (J(), Y("div", mg, [(J(!0), Y(q, null, K(ee.value, (e) => (J(), Y("article", { key: e.cache_key || `${e.provider}:${e.model}` }, [X("div", null, [
							X("strong", null, U(e.model || "model"), 1),
							X("small", null, U(fe(e)), 1),
							pe(e).length ? (J(), Y("small", hg, U(pe(e).join(" • ")), 1)) : Q("", !0),
							e.warning ? (J(), Y("small", gg, U(e.warning), 1)) : Q("", !0)
						]), e.unloadable && !e.managed ? (J(), Y("button", {
							key: 0,
							class: "tv-button danger",
							type: "button",
							disabled: !!u.value,
							onClick: (t) => Se(e)
						}, U(u.value === B(e.cache_key || e.model) ? "Unloading…" : "Unload"), 9, _g)) : (J(), Y("span", vg, U(e.managed ? "Managed" : "Loaded"), 1))]))), 128))])) : (J(), Y("div", yg, "No runtime models are loaded right now."))])
					]),
					X("article", bg, [X("header", null, [X("div", null, [
						n[8] ||= X("span", { class: "tv-eyebrow" }, "Orchestration", -1),
						n[9] ||= X("h2", null, "Hydra Jobs", -1),
						X("p", null, U(V(S.value.total)) + " total • Active turns " + U(M.value.length) + " • WebUI queue " + U(V(S.value.webui_jobs)) + " • Surface turns " + U(V(S.value.surface_running_turns)), 1)
					])]), X("section", xg, [n[10] ||= X("h3", null, "Active Turns", -1), M.value.length ? (J(), Y("div", Sg, [(J(!0), Y(q, null, K(M.value, (e) => (J(), Y("article", { key: e.id }, [
						X("header", null, [X("strong", null, U(e.task_name || "Hydra task"), 1), X("span", Cg, "Running " + U(ue(e.age_seconds)), 1)]),
						X("div", null, [
							X("span", null, U(e.platform_label || e.platform || "Unknown"), 1),
							e.source ? (J(), Y("span", wg, U(e.source), 1)) : Q("", !0),
							e.id ? (J(), Y("span", Tg, "Drop " + U(B(e.id).slice(0, 8)), 1)) : Q("", !0)
						]),
						e.current_tool ? (J(), Y("small", Eg, "Current verba/tool: " + U(e.current_tool), 1)) : Q("", !0),
						e.scope ? (J(), Y("small", Dg, "Scope: " + U(e.scope), 1)) : Q("", !0)
					]))), 128))])) : (J(), Y("div", Og, "No active Hydra turns right now."))])]),
					X("article", kg, [X("header", null, [X("div", null, [
						n[11] ||= X("span", { class: "tv-eyebrow" }, "Language models", -1),
						n[12] ||= X("h2", null, "LLM Calls", -1),
						X("p", null, U(V(C.value.active_total)) + " active • Started " + U(V(C.value.totals?.started)) + " • Completed " + U(V(C.value.totals?.completed)) + " • Failed " + U(V(C.value.totals?.failed)), 1)
					])]), X("section", Ag, [n[13] ||= X("h3", null, "Active Calls", -1), te.value.length ? (J(), Y("div", jg, [(J(!0), Y(q, null, K(te.value, (e, t) => (J(), Y("article", { key: e.id || t }, [X("div", null, [X("strong", null, U(e.source_label || e.label || "Unknown source"), 1), X("small", null, U(W(e, "llm")), 1)]), X("span", Mg, U(ue(e.age_seconds)), 1)]))), 128))])) : (J(), Y("div", Ng, "No active LLM calls right now."))])]),
					X("article", Pg, [X("header", null, [X("div", null, [
						n[14] ||= X("span", { class: "tv-eyebrow" }, "Vision", -1),
						n[15] ||= X("h2", null, "Vision Calls", -1),
						X("p", null, U(V(w.value.active_total)) + " active • Started " + U(V(w.value.totals?.started)) + " • Completed " + U(V(w.value.totals?.completed)) + " • Failed " + U(V(w.value.totals?.failed)), 1)
					])]), X("section", Fg, [n[16] ||= X("h3", null, "Active Calls", -1), N.value.length ? (J(), Y("div", Ig, [(J(!0), Y(q, null, K(N.value, (e, t) => (J(), Y("article", { key: e.id || t }, [X("div", null, [X("strong", null, U(e.source_label || e.label || "Unknown source"), 1), X("small", null, U(W(e, "vision")), 1)]), X("span", Lg, U(ue(e.age_seconds)), 1)]))), 128))])) : (J(), Y("div", Rg, "No active vision calls right now."))])]),
					X("article", zg, [X("header", null, [X("div", null, [
						n[17] ||= X("span", { class: "tv-eyebrow" }, "Prompt budget", -1),
						n[18] ||= X("h2", null, "Estimated Chat Context Window", -1),
						T.value.error ? (J(), Y("p", Bg, U(T.value.error), 1)) : V(T.value.prompt_tokens) || V(T.value.minimum_context_window) ? (J(), Y("p", Vg, U(P.value), 1)) : (J(), Y("p", Hg, "No estimate available yet. Send a chat message so Hydra can sample the active chat prompt stack."))
					])]), re.value.length && !T.value.error ? (J(), Y("section", Ug, [
						n[19] ||= X("h3", null, "Prompt Composition", -1),
						X("div", Wg, [(J(!0), Y(q, null, K(re.value, (e) => (J(), Y("article", { key: e.label }, [X("strong", null, U(e.label), 1), X("span", Gg, U(oe(e.tokens)), 1)]))), 128))]),
						X("small", null, "Active stack: " + U(V(T.value.enabled_verbas)) + " verbas enabled • " + U(V(T.value.connected_portals)) + " portals connected • " + U(V(T.value.running_cores)) + " cores running", 1),
						(J(!0), Y(q, null, K(F.value, (e) => (J(), Y("small", { key: e }, U(e), 1))), 128))
					])) : Q("", !0)])
				]))
			])]),
			_: 1
		}, 8, ["open"])], 64));
	}
}), qg = { class: "tater-vue-surface tvb-verbas" }, Jg = { class: "tv-page-heading" }, Yg = { class: "tv-heading-actions" }, Xg = { class: "tv-metrics" }, Zg = {
	key: 1,
	class: "tv-notice error"
}, Qg = {
	class: "tv-tabs tvb-tabs",
	"aria-label": "Verba sections"
}, $g = ["onClick"], e_ = { key: 0 }, t_ = {
	key: 2,
	class: "tvb-card-grid"
}, n_ = { class: "tv-eyebrow" }, r_ = { class: "tvb-version" }, i_ = {
	key: 0,
	class: "ti-tags"
}, a_ = ["onClick"], o_ = { key: 1 }, s_ = ["onClick"], c_ = {
	key: 0,
	class: "tv-empty"
}, l_ = {
	key: 3,
	class: "tvb-card-grid"
}, u_ = { class: "tv-eyebrow" }, d_ = { class: "tv-state" }, f_ = {
	key: 0,
	class: "ti-tags"
}, p_ = ["onClick"], m_ = {
	key: 0,
	class: "tv-empty"
}, h_ = {
	key: 4,
	class: "tvb-manage-list"
}, g_ = { class: "tv-panel tvb-manage-toolbar" }, __ = ["disabled"], v_ = { class: "ti-row-actions" }, y_ = ["disabled", "onClick"], b_ = ["onClick"], x_ = {
	key: 1,
	class: "ti-purge"
}, S_ = ["onUpdate:modelValue"], C_ = ["onClick"], w_ = {
	key: 3,
	class: "tv-state good"
}, T_ = {
	key: 0,
	class: "tv-empty"
}, E_ = {
	key: 5,
	class: "tv-panel tvb-repos"
}, D_ = { class: "ti-repo-row builtin" }, O_ = ["onClick"], k_ = {
	key: 0,
	class: "tv-empty compact"
}, A_ = { class: "tvb-repo-form" }, j_ = { class: "tv-eyebrow" }, M_ = { class: "tvb-field-grid" }, N_ = /* @__PURE__ */ ar({
	__name: "VerbasApp",
	props: {
		state: {},
		options: {}
	},
	setup(e) {
		let t = e, n = [
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
		], r = /* @__PURE__ */ G(n.some((e) => e.id === t.options.initialTab) ? String(t.options.initialTab) : "installed"), i = /* @__PURE__ */ G(""), a = /* @__PURE__ */ G(""), o = /* @__PURE__ */ G(""), s = /* @__PURE__ */ G({}), c = /* @__PURE__ */ G(""), l = /* @__PURE__ */ G(""), u = /* @__PURE__ */ G([]), d = /* @__PURE__ */ G(null), f = /* @__PURE__ */ G({}), p = $(() => t.state.payload?.runtime || {}), m = $(() => t.state.payload?.shop || {}), h = $(() => Array.isArray(p.value.items) ? p.value.items : []), g = $(() => Array.isArray(m.value.installed) ? m.value.installed : []), _ = $(() => Array.isArray(m.value.catalog) ? m.value.catalog : []), v = $(() => _.value.filter((e) => !e.installed).sort(E)), y = $(() => g.value.filter((e) => e.update_available)), b = $(() => h.value.filter((e) => !!e.enabled).length), x = $(() => new Map(h.value.map((e) => [T(e.id), e]))), S = $(() => {
			let e = /* @__PURE__ */ new Set(), t = g.value.map((t) => {
				let n = C(t.id || t.module_key || t.key);
				return e.add(T(n)), {
					id: n,
					runtime: x.value.get(T(n)) || null,
					shop: t
				};
			});
			return h.value.forEach((n) => {
				let r = C(n.id);
				r && !e.has(T(r)) && t.push({
					id: r,
					runtime: n,
					shop: null
				});
			}), t.sort((e, t) => D(e).localeCompare(D(t), void 0, {
				sensitivity: "base",
				numeric: !0
			}));
		});
		function C(e) {
			return String(e ?? "").trim();
		}
		function w(e) {
			return encodeURIComponent(C(e));
		}
		function T(e) {
			return C(e).toLowerCase();
		}
		function E(e, t) {
			return C(e.name || e.id).localeCompare(C(t.name || t.id), void 0, {
				sensitivity: "base",
				numeric: !0
			});
		}
		function D(e) {
			return C(e.runtime?.name || e.shop?.name || e.id);
		}
		function O(e) {
			return C(e.shop?.description || e.runtime?.description || "No description provided.");
		}
		function k(e) {
			return (Array.isArray(e.runtime?.platforms) && e.runtime?.platforms.length ? e.runtime.platforms : Array.isArray(e.shop?.platforms) ? e.shop.platforms : []).map((e) => C(e).replaceAll("_", " ")).filter(Boolean);
		}
		function A(e, n = "success") {
			a.value = e, o.value = n === "error" ? e : "", t.options.onToast?.(e, n);
		}
		function ee() {
			u.value = Array.isArray(m.value.repos?.additional) ? m.value.repos.additional.map((e) => ({ ...e })) : [];
		}
		async function j(e = !1) {
			e || (i.value = "Refreshing Verba…"), o.value = "";
			try {
				let [e, n] = await Promise.all([gs(t.options.endpoints.runtime), gs(t.options.endpoints.shop)]);
				t.state.payload = {
					runtime: e,
					shop: n
				}, ee();
			} catch (e) {
				A(e instanceof Error ? e.message : "Verba refresh failed.", "error");
			} finally {
				e || (i.value = "");
			}
		}
		async function M(e, n) {
			i.value = `${n ? "Enabling" : "Disabling"} ${e}…`;
			try {
				await _s(`${t.options.endpoints.runtime}/${w(e)}/enabled`, { enabled: n }), A(`${e} ${n ? "enabled" : "disabled"}.`), await j(!0), t.options.onHealthRefresh?.();
			} catch (e) {
				A(e instanceof Error ? e.message : "Verba toggle failed.", "error");
			} finally {
				i.value = "";
			}
		}
		async function te(e, n = "") {
			if (!(e === "remove" && !window.confirm(`Remove ${n}?${s.value[n] ? " Its saved data will also be deleted." : ""}`))) {
				i.value = `${e.replaceAll("-", " ")} ${n || "Verba"}…`, o.value = "";
				try {
					let i = n ? { id: n } : {};
					e === "remove" && (i.purge_redis = !!s.value[n]);
					let a = await _s(`${t.options.endpoints.shop}/${e}`, i), o = Array.isArray(a.updated) ? a.updated.length : 0, c = Array.isArray(a.failed) ? a.failed.length : 0, l = e === "update-all" ? `Update-all completed. Updated ${o}, failed ${c}.` : "Verba action completed.";
					A(C(a.message) || l, c ? "error" : "success"), await j(!0), e === "install" && (r.value = "installed"), t.options.onHealthRefresh?.();
				} catch (e) {
					A(e instanceof Error ? e.message : "Verba action failed.", "error");
				} finally {
					i.value = "";
				}
			}
		}
		function N(e) {
			let t = e.value ?? e.default ?? "", n = C(e.type).toLowerCase();
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
				let e = C(t);
				if (!e) return [];
				try {
					let t = JSON.parse(e);
					if (Array.isArray(t)) return t;
				} catch {}
				return e.split(",").map((e) => e.trim()).filter(Boolean);
			}
			return t;
		}
		function ne(e) {
			return (Array.isArray(e.show_when_all) ? e.show_when_all : e.show_when && typeof e.show_when == "object" ? [e.show_when] : []).every((e) => {
				let t = C(e.source_key ?? e.key);
				if (!t) return !0;
				let n = [
					...e.any_of || [],
					...e.values || [],
					...e.equals === void 0 ? [] : [e.equals],
					...e.value === void 0 ? [] : [e.value]
				].map((e) => String(e ?? "").trim());
				if (!n.length) return !0;
				let r = typeof f.value[t] == "boolean" ? f.value[t] ? "true" : "false" : String(f.value[t] ?? "").trim();
				return n.includes(r);
			});
		}
		function re(e) {
			d.value = e, f.value = Object.fromEntries((Array.isArray(e.settings) ? e.settings : []).filter((e) => C(e.key)).map((e) => [C(e.key), N(e)]));
		}
		async function P() {
			let e = d.value;
			if (e) {
				i.value = `Saving ${C(e.name || e.id)}…`;
				try {
					let n = Object.fromEntries((e.settings || []).filter((e) => {
						let t = C(e.type).toLowerCase();
						return C(e.key) && ![
							"section",
							"header",
							"readonly",
							"read_only",
							"led_preview"
						].includes(t) && ne(e);
					}).map((e) => [C(e.key), f.value[C(e.key)]]));
					await _s(`${t.options.endpoints.runtime}/${w(e.id)}/settings`, { values: n }), A(`Saved settings for ${C(e.name || e.id)}.`), d.value = null, await j(!0);
				} catch (e) {
					A(e instanceof Error ? e.message : "Settings save failed.", "error");
				} finally {
					i.value = "";
				}
			}
		}
		function F() {
			let e = l.value.trim();
			if (!e) {
				A("Repo URL is required.", "error");
				return;
			}
			if (u.value.some((t) => C(t.url).toLowerCase() === e.toLowerCase())) {
				A("That repository is already added.", "error");
				return;
			}
			u.value.push({
				name: c.value.trim(),
				url: e
			}), c.value = "", l.value = "", a.value = "Repository added. Save repositories to apply it.", o.value = "";
		}
		async function I() {
			i.value = "Saving Verba repositories…";
			try {
				await _s(`${t.options.endpoints.shop}/repos`, { repos: u.value }), A("Verba repositories saved."), await j(!0);
			} catch (e) {
				A(e instanceof Error ? e.message : "Repository save failed.", "error");
			} finally {
				i.value = "";
			}
		}
		function L(e) {
			e.key === "Escape" && (d.value = null);
		}
		return En(() => t.state.payload, ee, { deep: !1 }), ee(), window.addEventListener("keydown", L), Cr(() => window.removeEventListener("keydown", L)), (e, t) => (J(), Y("div", qg, [
			X("header", Jg, [t[8] ||= X("div", null, [
				X("span", { class: "tv-eyebrow" }, "Tater tools"),
				X("h1", null, "Verba"),
				X("p", null, "Enable Tater’s tools, manage their settings, and keep every Verba current.")
			], -1), X("div", Yg, [X("span", { class: z(["tv-live-pill", { busy: !!i.value }]) }, [t[7] ||= X("i", null, null, -1), Z(U(i.value || "Ready"), 1)], 2), X("button", {
				class: "tv-button",
				type: "button",
				onClick: t[0] ||= (e) => j()
			}, "Refresh")])]),
			X("div", Xg, [
				X("div", null, [t[9] ||= X("span", null, "Installed", -1), X("strong", null, U(g.value.length || h.value.length), 1)]),
				X("div", null, [t[10] ||= X("span", null, "Enabled", -1), X("strong", null, U(b.value), 1)]),
				X("div", null, [t[11] ||= X("span", null, "Store", -1), X("strong", null, U(_.value.length), 1)]),
				X("div", null, [t[12] ||= X("span", null, "Updates", -1), X("strong", null, U(Number(m.value.updates_available || y.value.length)), 1)])
			]),
			a.value || o.value ? (J(), Y("div", {
				key: 0,
				class: z(["tv-notice", { error: !!o.value }])
			}, U(o.value || a.value), 3)) : Q("", !0),
			m.value.errors?.length ? (J(), Y("div", Zg, U(m.value.errors.join(" • ")), 1)) : Q("", !0),
			X("nav", Qg, [(J(), Y(q, null, K(n, (e) => X("button", {
				key: e.id,
				type: "button",
				class: z({ active: r.value === e.id }),
				onClick: (t) => r.value = e.id
			}, [Z(U(e.label), 1), e.id === "manage" && y.value.length ? (J(), Y("span", e_, U(y.value.length), 1)) : Q("", !0)], 10, $g)), 64))]),
			r.value === "installed" ? (J(), Y("section", t_, [(J(!0), Y(q, null, K(S.value, (e) => (J(), Y("article", {
				key: e.id,
				class: "tv-panel tvb-verba-card"
			}, [
				X("header", null, [X("div", null, [X("span", n_, U(e.id), 1), X("h2", null, U(D(e)), 1)]), X("span", { class: z(["tv-state", { good: e.runtime?.enabled }]) }, U(e.runtime?.enabled ? "Enabled" : "Disabled"), 3)]),
				X("p", null, U(O(e)), 1),
				X("div", r_, [
					X("span", null, "Installed " + U(e.shop?.installed_ver || "0.0.0"), 1),
					X("span", null, "Store " + U(e.shop?.store_ver || "-"), 1),
					X("span", null, U(e.shop?.source_label || "local"), 1)
				]),
				k(e).length ? (J(), Y("div", i_, [(J(!0), Y(q, null, K(k(e).slice(0, 12), (e) => (J(), Y("span", { key: e }, U(e), 1))), 128))])) : Q("", !0),
				X("footer", null, [e.runtime?.settings?.length ? (J(), Y("button", {
					key: 0,
					class: "tv-button",
					type: "button",
					onClick: (t) => re(e.runtime)
				}, "Settings", 8, a_)) : (J(), Y("span", o_, U(e.runtime ? "No configurable settings" : "Runtime unavailable"), 1)), e.runtime ? (J(), Y("button", {
					key: 2,
					class: z(["tv-button", { primary: !e.runtime.enabled }]),
					type: "button",
					onClick: (t) => M(e.id, !e.runtime.enabled)
				}, U(e.runtime.enabled ? "Disable" : "Enable"), 11, s_)) : Q("", !0)])
			]))), 128)), S.value.length ? Q("", !0) : (J(), Y("div", c_, "No installed Verba found."))])) : r.value === "store" ? (J(), Y("section", l_, [(J(!0), Y(q, null, K(v.value, (e) => (J(), Y("article", {
				key: e.id,
				class: "tv-panel tvb-verba-card"
			}, [
				X("header", null, [X("div", null, [X("span", u_, U(e.id), 1), X("h2", null, U(e.name || e.id), 1)]), X("span", d_, "v" + U(e.version || "-"), 1)]),
				X("p", null, U(e.description || "No description provided."), 1),
				e.platforms?.length ? (J(), Y("div", f_, [(J(!0), Y(q, null, K(e.platforms.slice(0, 12), (e) => (J(), Y("span", { key: e }, U(C(e).replaceAll("_", " ")), 1))), 128))])) : Q("", !0),
				X("footer", null, [X("span", null, U(e.source_label || "Tater Shop"), 1), X("button", {
					class: "tv-button primary",
					type: "button",
					onClick: (t) => te("install", e.id)
				}, "Install", 8, p_)])
			]))), 128)), v.value.length ? Q("", !0) : (J(), Y("div", m_, "No additional Verba are available from the configured repositories."))])) : r.value === "manage" ? (J(), Y("section", h_, [
				X("div", g_, [X("div", null, [
					t[13] ||= X("span", { class: "tv-eyebrow" }, "Maintenance", -1),
					t[14] ||= X("h2", null, "Manage installed Verba", -1),
					X("p", null, U(y.value.length) + " update" + U(y.value.length === 1 ? "" : "s") + " available.", 1)
				]), X("button", {
					class: "tv-button primary",
					type: "button",
					disabled: !y.value.length,
					onClick: t[1] ||= (e) => te("update-all")
				}, "Update all", 8, __)]),
				(J(!0), Y(q, null, K(g.value.slice().sort(E), (e) => (J(), Y("article", {
					key: e.id,
					class: "tv-panel tvb-manage-row"
				}, [X("div", null, [X("strong", null, U(e.name || e.id), 1), X("span", null, U(e.installed_ver || "0.0.0") + " → " + U(e.store_ver || "-"), 1)]), X("div", v_, [
					X("button", {
						class: "tv-button",
						type: "button",
						disabled: !e.update_available,
						onClick: (t) => te("update", e.id)
					}, U(e.update_available ? "Update" : "Current"), 9, y_),
					x.value.has(T(e.id)) ? (J(), Y("button", {
						key: 0,
						class: "tv-button",
						type: "button",
						onClick: (t) => M(e.id, !x.value.get(T(e.id))?.enabled)
					}, U(x.value.get(T(e.id))?.enabled ? "Disable" : "Enable"), 9, b_)) : Q("", !0),
					e.required ? Q("", !0) : (J(), Y("label", x_, [bn(X("input", {
						"onUpdate:modelValue": (t) => s.value[e.id] = t,
						type: "checkbox"
					}, null, 8, S_), [[Jo, s.value[e.id]]]), t[15] ||= Z(" Delete data", -1)])),
					e.required ? (J(), Y("span", w_, "Required")) : (J(), Y("button", {
						key: 2,
						class: "tv-button danger",
						type: "button",
						onClick: (t) => te("remove", e.id)
					}, "Remove", 8, C_))
				])]))), 128)),
				g.value.length ? Q("", !0) : (J(), Y("div", T_, "No installed Verba found."))
			])) : (J(), Y("section", E_, [
				t[19] ||= X("header", null, [X("div", null, [
					X("span", { class: "tv-eyebrow" }, "Trusted sources"),
					X("h2", null, "Verba repositories"),
					X("p", null, "The built-in repository stays available. Add other trusted manifests below.")
				])], -1),
				X("article", D_, [X("div", null, [X("strong", null, U(m.value.repos?.default?.name || "Default"), 1), X("code", null, U(m.value.repos?.default?.url || "(not set)"), 1)]), t[16] ||= X("span", null, "Built-in", -1)]),
				(J(!0), Y(q, null, K(u.value, (e, t) => (J(), Y("article", {
					key: `${e.url}-${t}`,
					class: "ti-repo-row"
				}, [X("div", null, [X("strong", null, U(e.name || "Additional repository"), 1), X("code", null, U(e.url), 1)]), X("button", {
					class: "tv-button",
					type: "button",
					onClick: (e) => u.value.splice(t, 1)
				}, "Remove", 8, O_)]))), 128)),
				u.value.length ? Q("", !0) : (J(), Y("div", k_, "No additional repositories configured.")),
				X("div", A_, [
					X("label", null, [t[17] ||= X("span", null, "Name (optional)", -1), bn(X("input", {
						"onUpdate:modelValue": t[2] ||= (e) => c.value = e,
						type: "text",
						placeholder: "My Verba Repo"
					}, null, 512), [[qo, c.value]])]),
					X("label", null, [t[18] ||= X("span", null, "Repository URL", -1), bn(X("input", {
						"onUpdate:modelValue": t[3] ||= (e) => l.value = e,
						type: "url",
						placeholder: "https://example.com/verbas.json",
						onKeyup: cs(F, ["enter"])
					}, null, 544), [[qo, l.value]])]),
					X("button", {
						class: "tv-button",
						type: "button",
						onClick: F
					}, "Add"),
					X("button", {
						class: "tv-button primary",
						type: "button",
						onClick: I
					}, "Save repositories")
				])
			])),
			aa(sl, {
				open: !!d.value,
				onClose: t[6] ||= (e) => d.value = null
			}, {
				default: yn(() => [X("form", {
					class: "tv-modal tvb-settings-modal",
					onSubmit: os(P, ["prevent"])
				}, [
					X("header", null, [X("div", null, [X("span", j_, U(d.value?.id), 1), X("h2", null, U(d.value?.name || d.value?.id) + " settings", 1)]), X("button", {
						class: "tv-button",
						type: "button",
						onClick: t[4] ||= (e) => d.value = null
					}, "Close")]),
					X("div", M_, [(J(!0), Y(q, null, K(d.value?.settings || [], (e, n) => (J(), ea(Ru, {
						key: e.key || n,
						modelValue: f.value[e.key],
						"onUpdate:modelValue": (t) => f.value[e.key] = t,
						field: e,
						"all-values": f.value,
						onError: t[5] ||= (e) => A(e, "error"),
						onNotify: A
					}, null, 8, [
						"modelValue",
						"onUpdate:modelValue",
						"field",
						"all-values"
					]))), 128))]),
					X("footer", null, [X("span", null, U(i.value || a.value), 1), t[20] ||= X("button", {
						class: "tv-button primary",
						type: "submit"
					}, "Save settings", -1)])
				], 32)]),
				_: 1
			}, 8, ["open"])
		]));
	}
});
//#endregion
//#region src/entry.ts
function P_(e, t) {
	let n = /* @__PURE__ */ Ct({
		profile: t.initialProfile || {},
		messages: t.initialMessages || [],
		stats: t.initialStats || {
			enabled: !1,
			stats: null
		}
	}), r = fs(fc, {
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
function F_(e, t) {
	let n = /* @__PURE__ */ Ct({ payload: t.initialPayload }), r = fs(of, {
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
function I_(e, t) {
	let n = /* @__PURE__ */ Ct({ settings: t.initialSettings }), r = fs(Mp, {
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
function L_(e, t) {
	let n = /* @__PURE__ */ Ct({ payload: t.initialPayload }), r = fs(N_, {
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
function R_(e, t) {
	let n = /* @__PURE__ */ Ct({ payload: t.initialPayload }), r = fs(gm, {
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
function z_(e, t) {
	let n = /* @__PURE__ */ Ct({ payload: t.initialPayload }), r = fs(Ad, {
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
function B_(e, t) {
	let n = /* @__PURE__ */ Ct({ payload: t.initialPayload }), r = fs(Vh, {
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
function V_(e, t) {
	let n = /* @__PURE__ */ Ct({ summary: t.initialSummary || {} }), r = fs(Yh, {
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
function H_(e, t) {
	let n = /* @__PURE__ */ Ct({
		health: t.initialState?.health || null,
		text: t.initialState?.text || "Checking system…",
		tone: t.initialState?.tone || "normal"
	}), r = fs(Kg, {
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
function U_(e, t) {
	let n = /* @__PURE__ */ Ct({ payload: t.initialPayload }), r = fs(uu, {
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
export { P_ as mountChat, z_ as mountCores, F_ as mountDashboard, I_ as mountIntegrations, U_ as mountMusicCore, R_ as mountPortals, H_ as mountRuntimeStatus, V_ as mountSettings, B_ as mountSpudex, L_ as mountVerbas };
