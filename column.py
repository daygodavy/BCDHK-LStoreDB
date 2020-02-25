from page import Page
from bufferpool import bp

class Column:

    def __init__(self):
        # the pages of the column
        #self.pages = [Page()]

        # the index of the last base page in the column
        self.last_base_page = 0

        # the index of the last page of the column
        self.last_page = 0

    def add_base_value(self, value, page_range):
        """
        Add a record to a the base pages

        :param value: int                   # the value to be added to the base pages

        :return: int                        # the page number of the page being written too
        :return: int                        # the offset of the write operation
        """
        # page = self.pages[self.last_base_page]
        # offset = 0
        #
        # # if page has capacity
        # if page.has_capacity():
        #
        #     # write to it
        #     offset = page.write(value)
        #
        # # otherwise create a new one and add it
        # else:
        #     page = Page()
        #     offset = page.write(value)
        #     self.last_base_page += 1
        #     self.pages.insert(self.last_base_page, page)
        #     self.last_page += 1
        page_num, offset = bp.write_object(page_range, value)

        return page_num, offset

    def add_tail_value(self, value):
        """
        Add a record to the tail pages

        :param value: int                   # the value to be added to the base pages

        :return: int                        # the page number of the page being written too
        :return: int                        # the offset of the write operation
        """
        page = 0
        offset = 0

        # if there are no tail pages yet
        if self.last_page == self.last_base_page:

            # create a tail page
            page = Page()
            self.pages.append(page)
            self.last_page += 1

        # otherwise the page we want is the last one
        else:
            page = self.pages[-1]

        # if the page has capacity
        if page.has_capacity():

            # write to it
            offset = page.write(value)

        # otherwise add a new tail page
        else:
            page = Page()
            offset = page.write(value)
            self.pages.append(page)
            self.last_page += 1

        return self.last_page, offset

    def update_value(self, page_number, offset, value):
        """
        Update the base record value of a record

        :param page_number: int             # the page number by which we are updating a record
        :param offset: int                  # the offset in the page of the record to be updated
        :param value: int                   # the value to be written to the relevant page and offset
        """
        self.pages[page_number].overwrite(offset, value)

    def delete(self, page_number, offset):
        """
        lazy deletion of the base record from the column

        :param page_number: int             # the page number from which to delete the record
        :param offset: int                  # the offset of the record to be deleted
        """
        self.pages[page_number].overwrite(offset, 0)

    def read(self, page_number, offset, page_range):
        """
        read a value

        :param page_range: page_range obj
        :param page_number: int
        :param offset: int
        :return: int
        """
        read = bp.read_object(page_range=page_range, page_num=page_number, offset=offset)
        #print("READ:", read)
        # return self.pages[page_number].read(offset)
        return read
