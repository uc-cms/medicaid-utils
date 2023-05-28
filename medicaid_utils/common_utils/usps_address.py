#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""This script shows an example of using `requests` and the USPS Address
Information API. In order to use this, you must first register so you can
get your USERID.  Your ID must be in the environment variable `USPS_USERID`.
For information on the API see `here <https://www.usps.com/business/web-tools-apis/address-information-api.htm>_`
"""

# Imports #####################################################################
import os
import xml.dom.minidom
import xml.etree.ElementTree as xml_et
from dotenv import load_dotenv

import requests

load_dotenv()
# Globals #####################################################################
USPS_USERID = os.environ["USPS_USERID"]


def dump_xml(raw_xml):
    """
    Return a string representation of XML with proper intendation
    """
    if isinstance(raw_xml, xml_et.Element):
        raw_xml = xml_et.tostring(raw_xml)

    return xml.dom.minidom.parseString(raw_xml).toprettyxml()


def get_text(root, xpath):
    """
    Return the text of the XPath element, or None if the element was not found.
    """
    element = root.find(xpath)

    if element is not None:
        return element.text

    return ""


class USPSAddress(object):
    """Representation of an United States Postal Service address."""

    def __init__(
        self, name="", suite="", street="", city="", state="", zip5="", zip4=""
    ):
        self.name = name
        self.suite = suite  # or APT
        self.street = street.title()
        self.city = city.title()
        self.state = state.upper() if len(state) == 2 else state.title()
        self.zip5 = zip5
        self.zip4 = zip4

        self._standardized = ""

    def zipcode(self):
        """Returns the zipcode based on whether or not `zip4` is used."""
        if self.zip4:
            return "%s-%s" % (self.zip5, self.zip4)

        return self.zip5

    def original(self):
        """Return the non-standardized address format"""
        lines = [
            self.name,
            self.suite,
            self.street,
            "{0}, {1}  {2}".format(self.city, self.state, self.zipcode()),
        ]

        return "\n".join([x for x in lines if x])

    def standardized(self):
        """Return the standardized address format"""
        if self._standardized:
            return self._standardized

        api = AddressStandardizationWebTool(
            self.street,
            self.city,
            self.state,
            self.name,
            self.suite,
            self.zip5,
            self.zip4,
        )

        result, warning, error = api.get_standardized_address()
        lines = []
        if not bool(error):
            lines = [result["name"]]

            if result["suite"]:
                lines.append("%s %s" % (result["street"], result["suite"]))
            else:
                lines.append(result["street"])

            if result["zip4"]:
                lines.append(
                    "%s %s %s-%s"
                    % (
                        result["city"],
                        result["state"],
                        result["zip5"],
                        result["zip4"],
                    )
                )
            else:
                lines.append(
                    "%s %s %s"
                    % (result["city"], result["state"], result["zip5"])
                )

        self._standardized = " ".join([x for x in lines if x])
        self._warning = warning
        self._error = error
        return self._standardized


class USPSShippingAPI(object):
    """
    Representation of the USPS Shipping API
    https://www.usps.com/business/web-tools-apis/address-information-api.htm
    """

    url = "http://production.shippingapis.com/ShippingAPI.dll"

    def __init__(self, api, userid=USPS_USERID):
        self.userid = userid
        self.api = api

    def _xml_payload(self):
        """
        Child classes should override this to return a string appropriate for
        its API.
        """
        raise Exception("Must override this function!")

    def send_request(self):
        """
        Send the request and return the XML response.
        """
        response = requests.get(
            USPSShippingAPI.url,
            params={"API": self.api, "XML": self._xml_payload()},
        )

        root = xml_et.fromstring(response.content)
        error = ""
        if bool(
            (response.status_code != 200)
            or (root.tag.lower() == "error")
            or root.findall(".//Error")
        ):
            error = get_text(root, "./Address/Error/Description")

        # Check for any returned messages.  There's shouldn't be any, so treat
        # it as a warning.
        warning = root.find(".//ReturnText")
        warning = warning.text if warning is not None else ""

        return root, warning, error


class AddressStandardizationWebTool(USPSShippingAPI):
    """
    Object to get a standardized USPS Address.
    """

    api = "Verify"

    def __init__(
        self,
        street,
        city,
        state,
        name=None,
        suite=None,
        zip5=None,
        zip4=None,
        userid=USPS_USERID,
    ):
        super(AddressStandardizationWebTool, self).__init__(
            userid=userid, api=AddressStandardizationWebTool.api
        )

        self.name = name
        self.suite = suite  # or APT
        self.street = street
        self.city = city
        self.state = state
        self.zip5 = zip5
        self.zip4 = zip4

    def get_standardized_address(self):
        """
        Returns a standardized format of the object's address.
        """
        root, warning, error = self.send_request()
        standardized = {}
        if not bool(error):
            standardized = {
                "name": get_text(root, "./Address/FirmName"),
                "suite": get_text(root, "./Address/Address1"),
                "street": get_text(root, "./Address/Address2"),
                "city": get_text(root, "./Address/City"),
                "state": get_text(root, "./Address/State"),
                "zip5": get_text(root, "./Address/Zip5"),
                "zip4": get_text(root, "./Address/Zip4"),
            }
        return standardized, warning, error

    def _xml_payload(self):
        """
        Returns the appropriate XML format for an 'Address Standardization Web
        Tool' request.
        """
        root = xml_et.Element("AddressValidateRequest")
        root.set("USERID", self.userid)

        include_opt = xml_et.Element("IncludeOptionalElements")
        include_opt.text = "false"
        root.append(include_opt)

        return_carrier_route = xml_et.Element("ReturnCarrierRoute")
        return_carrier_route.text = "false"
        root.append(return_carrier_route)

        address = xml_et.Element("Address")
        address.set("ID", "0")

        firm_element = xml_et.Element("FirmName")
        firm_element.text = self.name
        address.append(firm_element)

        address1_element = xml_et.Element("Address1")
        address1_element.text = self.suite
        address.append(address1_element)

        address2_element = xml_et.Element("Address2")
        address2_element.text = self.street
        address.append(address2_element)

        city_element = xml_et.Element("City")
        city_element.text = self.city
        address.append(city_element)

        state_element = xml_et.Element("State")
        state_element.text = self.state
        address.append(state_element)

        zip5_element = xml_et.Element("Zip5")
        zip5_element.text = self.zip5
        address.append(zip5_element)

        zip4_element = xml_et.Element("Zip4")
        zip4_element.text = self.zip4
        address.append(zip4_element)

        root.append(address)

        return xml_et.tostring(root)
