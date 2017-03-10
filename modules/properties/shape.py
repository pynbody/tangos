from . import HaloProperties
import numpy as np
import math
import pynbody


class Ellipsoids(HaloProperties):

    def spherical_region(self):
        return True

    def requires_property(self):
        return ["SSC"]

    def preloop(self, f, name, ar):
        f.dm['smooth']
        f.dm['rho']
        f.dm['smooth'] *= 5
        f.dm['smooth'][np.where(f.dm['smooth'] < 0.3)] = 0.3

    def get_vals_and_vecs_thresholded(self, im, im_name="phi"):
        import aniso_oscillator
        x, y, z = np.mgrid[-75.:75., -75.:75., -75.:75.]
        x *= 40. / 150
        y *= 40. / 150
        z *= 40. / 150
        # use an annulus to get a good threshold value
        r = np.sqrt(x ** 2 + y ** 2 + z ** 2)
        sel = (r > 14.0) * (r < 15.0)
        phi_threshold = im[sel].mean()
        ready = False

        while not ready:
            print "threshold=", phi_threshold
            if im_name == "phi":
                im_thresh = im < phi_threshold
            else:
                im_thresh = im > phi_threshold

            if im_thresh[75, 75, 75] == 0:
                phi_threshold *= 1.01
            else:
                ready = True

        # now identify only the connected part
        import scipy.ndimage.measurements
        im_lab, n = scipy.ndimage.measurements.label(im_thresh)
        r = im_lab[75, 75, 75]
        print "Want region", r, "of", n
        im_thresh = im_lab == r
        qmom = aniso_oscillator.quadrumom(im_thresh)
        vals, vecs = np.linalg.eig(qmom)
        # convention: put vecs into northern hemisphere
        # vecs*=(vecs[2]/abs(vecs[2]))[np.newaxis,:]
        return vals, vecs, phi_threshold

    def get_vals_and_vecs(self, im, im_name="phi"):
        import aniso_oscillator
        x, y, z = np.mgrid[-75.:75., -75.:75., -75.:75.]
        x *= 40. / 150
        y *= 40. / 150
        z *= 40. / 150
        qmom = aniso_oscillator.quadrumom(im)
        vals, vecs = np.linalg.eig(qmom)
        # convention: put vecs into northern hemisphere
        # vecs*=(vecs[2]/abs(vecs[2]))[np.newaxis,:]
        return vals, vecs


    def run(self, f, im_name, hnum=0):

        im = pynbody.sph.to_3d_grid(
            f.dm, nx=150, x2=20.0, qty=im_name, approximate_fast=True, threaded=8, denoise=True)
        self.im = im
        np.save(f.ancestor.filename + "." + im_name +
                "." + str(hnum) + "-grid.npy", im)
        return self.get_vals_and_vecs(im, im_name)

    def calculate(self, f, exist):
        f.dm['pos'] -= exist['SSC']
        rho_val, rho_vec = self.run(f, 'rho', exist.halo_number)
        try:
            f.dm['phi']
            phi_val, phi_vec = self.run(
                f, 'phi', exist.halo_number)
        except KeyError:
            phi_val, phi_vec, phi_thresh = 0, 0, 0
        f.dm['pos'] += exist['SSC']
        return phi_val, phi_vec, rho_val, rho_vec

    @classmethod
    def name(self):
        return "tellipsoid_phi_vals", "tellipsoid_phi_vecs", \
            "tellipsoid_rho_vals", "tellipsoid_rho_vecs"
